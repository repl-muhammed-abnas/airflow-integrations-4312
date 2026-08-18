from collections import defaultdict
from uuid import uuid4
from ast import literal_eval
from datetime import datetime, date
import rail

MANDATORY_FIELDS = {
    'employee_id': 'EmployeeCode',
    'entry_date': 'LogDate',
    'punch_time': 'log Time'
}

FEED_ENTRYDATE_DATE_FORMAT = "%m/%d/%Y"
FEED_ENTRYTIME_TIME_FORMAT = "%H:%M:%S"

PUNCH_IN = "punch_in"
PUNCH_OUT = "punch_out"

null = None

def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"{log_value} not present in the input")
    return rail.smartjoin_by_delim(missing_fields, ";")

def parse_date(date_value, date_format):
    return datetime.strptime(date_value, date_format)

def parse_date_json(date_value: dict):
    return date(day=date_value['day'], month=date_value['month'], year=date_value['year'])

def parse_time(time_value, time_format):
    return datetime.strptime(time_value, time_format).time()

def get_entry_date(item):
    return (item['entry_date'], parse_date(item['entry_date'], FEED_ENTRYDATE_DATE_FORMAT))

def parse_date_to_str(date_value, date_format):
    return date_value.strftime(date_format)

def parse_date_in_same_format(date_value):
    date_value = parse_date(date_value, FEED_ENTRYDATE_DATE_FORMAT)
    return parse_date_to_str(date_value, FEED_ENTRYDATE_DATE_FORMAT)

def parse_time_in_same_format(time_value):
    return time_value.strftime(FEED_ENTRYTIME_TIME_FORMAT)

def get_max_min_date_for_user():
        per_user_data = rail.load_all_records(rail.result("get_all_records_for_user"))
        user_records = sorted(list((map(get_entry_date, per_user_data))), key=lambda x: x[1])
        min_date = null
        max_date = null
        if user_records:
            min_date = user_records[0][0]
            max_date = user_records[-1][0]
        return (min_date, max_date)

def get_min_max_punches(per_user_data):
    in_out_dict = defaultdict(lambda: {
            PUNCH_IN: null,
            PUNCH_OUT: null
        })
    for item in per_user_data:
        log_time = parse_time(item["punch_time"], FEED_ENTRYTIME_TIME_FORMAT)
        date = item["entry_date"]
        if in_out_dict[date][PUNCH_IN] is null or log_time < in_out_dict[date][PUNCH_IN]:
            in_out_dict[date][PUNCH_IN] = log_time
        if in_out_dict[date][PUNCH_OUT] is null or log_time > in_out_dict[date][PUNCH_OUT]:
            in_out_dict[date][PUNCH_OUT] = log_time
    return {
        parse_date_in_same_format(date): {
            PUNCH_IN: parse_time_in_same_format(min_max[PUNCH_IN]),
            PUNCH_OUT: parse_time_in_same_format(min_max[PUNCH_OUT])
        } for date, min_max in in_out_dict.items()
    }

def get_punch_in_out_for_each_date():
    per_user_data = rail.load_all_records(rail.result("get_all_records_for_user"))
    all_punches = get_min_max_punches(per_user_data)
    for entry_date, punches in all_punches.items():
        if punches[PUNCH_IN] == punches[PUNCH_OUT]:
            punches[PUNCH_OUT] = null
    return all_punches

def map_timesheet_with_user_data(punch_in_out_for_each_date, ts_data_task_id):
    ts_data = rail.result(ts_data_task_id)
    punch_data = rail.result(punch_in_out_for_each_date)

    def get_map_data(item):
        timesheet_data = get_timesheet_data(item[0], ts_data)
        return {
            **{
                item[0]:item[1]
            },
            **timesheet_data
        }
    user_ts_data = list(map(get_map_data, punch_data.items()))
    ts_present_and_to_reopen = filter(lambda record: record['timesheet_found'].lower()=="yes" and
                                      record['timesheet_status_uri'].split(':')[-1] not in ['open', 'rejected'], user_ts_data)
    timesheet_to_reopen = list(map(lambda ts_to_reopen: {
                            "ts_uri": ts_to_reopen['timesheet_uri'],
                            "timesheet_status_uri": ts_to_reopen['timesheet_status_uri'],
                            "timesheet_status": ts_to_reopen['timesheet_status'],
                            "user_uri": rail.result('get_user_uri'),
                            "uuid": str(uuid4())
                        }, ts_present_and_to_reopen))

    rail.set_result(key="timesheet_to_reopen",val= list({v['ts_uri']:v for v in timesheet_to_reopen}.values()))
    return rail.write_json_artifact(user_ts_data)

def map_time_punch_data(punch_in_out_for_each_date, time_punch_task_id):
    all_replicon_time_punch_data = rail.result(time_punch_task_id)
    feed_punch_data = rail.result(punch_in_out_for_each_date)
    replicon_time_punches = {}
    dummy = {
        "in": PUNCH_IN,
        "out": PUNCH_OUT
    }
    for item in all_replicon_time_punch_data:
        dt = parse_date_in_same_format(f"{item['punchTime']['month']}/{item['punchTime']['day']}/{item['punchTime']['year']}")
        data = {
                "action":dummy[item['actionUri'].split(':')[-1]],
                "punch_uri": item['uri']
            } if dummy.get(item['actionUri'].split(':')[-1]) else {} # checking if action is only in/out ignoring break punches
        if data: # checking if is any in/out action present
            if dt in replicon_time_punches:
                replicon_time_punches[dt].append(data)
            else:
                replicon_time_punches[dt] = [data]
    punch_to_delete = []
    for entry_date, log_time in feed_punch_data.items():
        if entry_date in replicon_time_punches and log_time[PUNCH_IN]:
            punch_to_delete += [punchs['punch_uri'] for punchs in replicon_time_punches[entry_date]]
    rail.set_result(key="punch_to_delete",val = punch_to_delete)
    return replicon_time_punches

def is_date_in_ts_period(ts_dates, entry_date):
    return parse_date_json(ts_dates['startDate']) <= parse_date(entry_date, FEED_ENTRYDATE_DATE_FORMAT).date() <= parse_date_json(ts_dates['endDate'])

def get_timesheet_data(entry_date, ts_data):
    timesheet_record = list(filter(lambda ts: is_date_in_ts_period(
        ts['timesheet_date_range'], entry_date), ts_data))
    user_uri = rail.result('get_user_uri')
    if not timesheet_record:
        return {
            "timesheet_found": "No",
            "timesheet_uri": "na",
            "timesheet_status_uri": "na",
            "user_uri": user_uri
        }
    return {
        **{
            "timesheet_found": "Yes",
            "user_uri": user_uri
        },
        **timesheet_record[0]
    }

def prepare_to_add_in_log(punch_in_out_for_each_date):
    feed_punch_data = rail.result(punch_in_out_for_each_date)
    response = []
    for entry_date, log_time in feed_punch_data.items():
        response.append({
            "entry_date": entry_date,
            "punch_time": log_time[PUNCH_IN],
        })
        if log_time[PUNCH_OUT]:
            response.append({
                "entry_date": entry_date,
                "punch_time": log_time[PUNCH_OUT]
            })
    return response

def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        elif isinstance(userlogs, str) and userlogs[0] == '[':
            userlogs = literal_eval(userlogs)
            log_artifacts.extend(userlogs)
        else:
            userlogs = literal_eval(userlogs)
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
            **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: x['status'] == 'Skipped', final_log_records ))))

    return  final_log_records
