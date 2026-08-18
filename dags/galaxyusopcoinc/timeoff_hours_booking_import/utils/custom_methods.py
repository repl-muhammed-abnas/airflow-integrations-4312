from datetime import datetime
import pendulum
import json
import rail
from dateutil.relativedelta import relativedelta

null = None

FEED_ENTRYDATE_DATE_FORMAT = '%Y-%m-%d'

MANDATORY_FIELDS = {
    "employee_id": "employee_id",
    "booking_id": "booking_id",
    "plan_ref_id": "plan_ref_id",
    "request_type": "request_type",
    "timeoff_date": "timeoff_date",
    "hours": "hours"
}


def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
        if item['hours'] == '0':
            missing_fields.append("Record processing skipped as received timeoff hours value is zero")
            break
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    prev_2_hours_datetime = (
        today - relativedelta(hours=2, minute=0, second=0, microsecond=0)).isoformat()
    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "log_filename": 'Timeoff_Sync_HIBOB_to_Replicon_Logs_' + today.strftime("%Y%m%d_%H%M%S") + '.csv',
        "prev_2_hours_encoded": prev_2_hours_datetime.replace(":", "%3A").replace("+", "%2B")
    }


def is_entry_date_less_than_startdate(start_date, end_date):
    return datetime.fromisoformat(end_date) < datetime.fromisoformat(start_date)


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
