from datetime import datetime, date, timedelta
from copy import deepcopy
from uuid import uuid4
import re
from calendar import monthrange
import json
import rail

null = None

FEED_ENTRYDATE_DATE_FORMAT = '%Y-%m-%d'


def parse_date(date_value, date_format):
    return datetime.strptime(date_value, date_format)


def parse_date_json(date_value: dict):
    return date(day=date_value['day'], month=date_value['month'], year=date_value['year'])


MANDATORY_FIELDS = {
    "employee_id": "employee_id",
    "startdate": "startdate",
    "enddate": "enddate",
    "timeofftype": "timeofftype",
    "hours": "hours"
}


def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if item['hours'] and float(item['hours']) <= 0:
            missing_fields.append(
                "Record processing skipped as received timeoff hours value is zero/negative")
            break
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_timeoff_dates_exception_message(item):
    if 'invalid' in item['validation']:
        return "The received timeoff start date is greater than timeoff end date"
    if 'start_date' in item['validation']:
        return "The received timeoff start date is before the user start date in replicon"
    return "The received timeoff end date is after the user end date in replicon"

def is_date_in_ts_period(ts_dates, startdate, enddate):
    return parse_date(startdate, FEED_ENTRYDATE_DATE_FORMAT).date() <= parse_date_json(ts_dates['endDate']) and parse_date(
        enddate, FEED_ENTRYDATE_DATE_FORMAT).date() >= parse_date_json(ts_dates['startDate'])

def get_timesheet_data(startdate, enddate, ts_data):
    timesheet_record = list(filter(lambda ts: is_date_in_ts_period(
        ts['timesheet_date_range'], startdate, enddate), ts_data))
    if not timesheet_record:
        return {
            "timesheet_found": "No",
            "timesheet_uri": "na",
            "timesheet_status_uri": "na"
        }
    return {
        **{
            "timesheet_found": "Yes"
        },
        **timesheet_record[0]
    }


def map_timesheet_with_user_data(user_data_task_id, ts_data_task_id):
    user_data = rail.load_all_records(rail.result(user_data_task_id))
    ts_data = rail.result(ts_data_task_id)

    unique_entry_ids = list(
        set(map(lambda record: record['booking_id'], user_data)))

    def get_all_data_for_entry_id(item_entry_id):
        data_for_item_entry_id = list(
            filter(lambda rec: rec['booking_id'] == item_entry_id, user_data))
        return {
            **data_for_item_entry_id[0], }

    user_ts_data_per_unique_entry_id = list(
        map(get_all_data_for_entry_id, unique_entry_ids))

    def get_map_data(item):
        # Get ALL timesheets that overlap with this timeoff period
        overlapping_timesheets = list(filter(lambda ts: is_date_in_ts_period(
            ts['timesheet_date_range'], item['startdate'], item['enddate']), ts_data))

        if not overlapping_timesheets:
            return [{
                **item,
                "timesheet_found": "No",
                "timesheet_uri": "na",
                "timesheet_status_uri": "na"
            }]

        # Create one record per overlapping timesheet
        return list(map(lambda ts: {
            **item,
            "timesheet_found": "Yes",
            **ts
        }, overlapping_timesheets))

    # Flatten the list of lists into a single list
    user_ts_data = [record for item in user_ts_data_per_unique_entry_id
                    for record in get_map_data(item)]

    ts_present_and_to_reopen = filter(lambda record: record['timesheet_found'].lower() == "yes" and
                                      record['timesheet_status_uri'].split(':')[-1] not in ['open', 'rejected'], user_ts_data)
    timesheet_to_reopen = list(map(lambda ts_to_reopen: {
        "ts_uri": ts_to_reopen['timesheet_uri'],
        "timesheet_status_uri": ts_to_reopen['timesheet_status_uri'],
        "timesheet_status": ts_to_reopen['timesheet_status'],
        "timesheet_period": ts_to_reopen['timesheet_period'],
        "user_uri": ts_to_reopen["user_uri"],
        "unit_of_work_id": str(uuid4())
    }, ts_present_and_to_reopen))

    rail.set_result(key="timesheet_to_reopen", val=list(
        {v['ts_uri']: v for v in timesheet_to_reopen}.values()))
    return rail.write_json_artifact(user_ts_data)


def convert_sting_to_date(date_string):
    return datetime.strptime(date_string, FEED_ENTRYDATE_DATE_FORMAT).strftime("%Y-%m-%d")


def is_entry_date_less_than_startdate(start_date, end_date):
    return date.fromisoformat(str(end_date)) < date.fromisoformat(str(start_date))

def parse_date(date_str, fmt="%m/%d/%Y"):
    return datetime.strptime(date_str, fmt)

def format_date(date_obj, fmt="%m/%d/%Y"):
    return date_obj.strftime(fmt)

def adjust_timeoff_records(dag_run):
    final_result = []
    within_range_records = []

    range_start = datetime.strptime(dag_run.conf['startdate'], FEED_ENTRYDATE_DATE_FORMAT)
    range_end = datetime.strptime(dag_run.conf['enddate'], FEED_ENTRYDATE_DATE_FORMAT)

    for record in rail.result("get_time_off_booking_details")['timeoff_details']:
        start_date = parse_date(record['start_date'])
        end_date = parse_date(record['end_date'])

        if end_date < range_start or start_date > range_end:
            final_result.append(record)

        elif range_start <= start_date and end_date <= range_end:
            within_range_records.append(record)

        elif start_date < range_start <= end_date <= range_end:
            trimmed = deepcopy(record)
            trimmed['end_date'] = format_date(range_start - timedelta(days=1))
            final_result.append(trimmed)

        elif range_start <= start_date <= range_end < end_date:
            trimmed = deepcopy(record)
            trimmed['start_date'] = format_date(range_end + timedelta(days=1))
            final_result.append(trimmed)

        elif start_date < range_start and end_date > range_end:
            before_range = deepcopy(record)
            after_range = deepcopy(record)

            before_range['end_date'] = format_date(range_start - timedelta(days=1))
            after_range['start_date'] = format_date(range_end + timedelta(days=1))
            after_range['timeoff_uri'] = None

            final_result.extend([before_range, after_range])

    return {
        'timeoff_to_delete': within_range_records,
        'timeoff_to_update': final_result
    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    logs = rail.result("create_log")

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="total_record_count", val=rail.result(
        "query_sum_of_hours_from_raw_data", key="length"))

    return final_log_records


def get_final_payload_sendemail():
    final_payload = {"email": {
        "to": [
            {
                "user": {
                    "uri": rail.result("get_user_details")['uri'],
                    "loginName": null
                },
                "email": null
            }
        ],
        "cc": [],
        "bcc": [],
        "replyTo": null,
        "fromDisplayName": null,
        "subject": rail.get_company_key() + '| Timesheet was reopened in Replicon - ' + datetime.now().strftime("%Y-%m-%dT%H%M%S%z"),
        "htmlBody": rail.result("get_email_body"),
        "textBody": null,
        "attachments": []
    }}
    return json.dumps(final_payload)


def extract_date_from_filename(filename):
    match = re.search(r'(\d{8})', filename)
    if not match:
        raise ValueError(f"Unable to extract date from filename: {filename}")

    date_str = match.group(1)
    # Parse to date object
    return datetime.strptime(date_str, '%Y%m%d').date()


def calculate_three_month_window(file_date):
    # Get first day of current month
    current_month_start = file_date.replace(day=1)

    # Calculate M-1 (previous month start)
    if current_month_start.month == 1:
        # If January, previous month is December of previous year
        previous_month_start = date(current_month_start.year - 1, 12, 1)
    else:
        previous_month_start = date(current_month_start.year, current_month_start.month - 1, 1)

    # Calculate M+1 (next month end)
    if current_month_start.month == 12:
        # If December, next month is January of next year
        next_month = 1
        next_month_year = current_month_start.year + 1
    else:
        next_month = current_month_start.month + 1
        next_month_year = current_month_start.year

    # Calculate M+2 to get the last day of M+1
    if next_month == 12:
        next_next_month = 1
        next_next_month_year = next_month_year + 1
    else:
        next_next_month = next_month + 1
        next_next_month_year = next_month_year

    # Last day of M+1 is one day before the first day of M+2
    next_month_end = date(next_next_month_year, next_next_month, 1) - timedelta(days=1)

    return {
        'window_start': previous_month_start,
        'window_end': next_month_end,
        'current_month': current_month_start,
        'file_date': file_date
    }


def check_if_any_record_needs_auto_approval(query_task_id):
    records = rail.load_all_records(rail.result(query_task_id))
    return any(record.get('needs_auto_approval', False) for record in records)


def filter_timesheets_needing_approval(timesheet_data):
    timesheets_to_approve = []

    for ts in timesheet_data:
        ts_status = ts['timesheet_status_uri'].split(':')[-1].lower()

        # Only approve timesheets that are not already approved, open, or rejected
        if ts_status in ['open']:
            timesheets_to_approve.append({
                'ts_uri': ts['timesheet_uri'],
                'timesheet_period': ts['timesheet_period'],
                'timesheet_status': ts['timesheet_status'],
                'unit_of_work_id': str(uuid4())
            })

    return timesheets_to_approve
