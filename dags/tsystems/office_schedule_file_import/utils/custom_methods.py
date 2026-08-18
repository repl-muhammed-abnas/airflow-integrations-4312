import rail
from datetime import datetime, timedelta
from pendulum import now
from ast import literal_eval

MANDATORY_FIELDS = {
    "employee_id": "CID",
    "valid_from": "Valid From",
    "schedule_name": "Schedule Name",
}


def get_missing_field_message(item):
    error_msg = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            error_msg.append(f"{log_value} not present in the input")

        if key == 'schedule_name' and item[key].count(',') != 1:
            error_msg.append(f"{log_value} is not valid")
        if key == 'schedule_name' and item[key].count(',') == 1:
            part1, part2 = item[key].split(',')
            if not part1.strip() or not part2.strip():
             error_msg.append(f"{log_value} is not valid")
        

    return rail.smartjoin_by_delim(error_msg, ";")


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    assignmentlogs = rail.result("gather_assignment_logs")
    otherlogs = rail.result("create_log")

    if assignmentlogs:
        if isinstance(assignmentlogs, list):
            log_artifacts.extend(assignmentlogs)
        elif isinstance(assignmentlogs, str) and assignmentlogs[0] == '[':
            assignmentlogs = literal_eval(assignmentlogs)
            log_artifacts.extend(assignmentlogs)
        else:
            log_artifacts.append(assignmentlogs)

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
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{"ecid": log['ecid']},
        **dict(log['properties'].items()),
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))

    return final_log_records

def get_email_details_callable(dag_run, timezone):
    _now = now(timezone)
    return {
        "job_end_time" : (_now).isoformat(),
        "job_duration": (((_now - datetime.strptime(rail.result('job_started_time'), "%y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": "Log_"+ rail.render_template("{{ result('new_file_sensor') | file_name }}") + _now.strftime('%y%m%dT%H%M%S')+".csv"
    }

def check_if_valid_from_date_is_less_than_user_start_date(dag_run):
    start_date_json = rail.result("get_user_details")[0][
        "userDetails"]["employmentDateRange"]["startDate"]
    user_start_date = datetime(
        start_date_json["year"], start_date_json["month"], start_date_json["day"])
    valid_from = datetime.strptime(dag_run.conf["valid_from"], "%d.%m.%Y")
    if valid_from < user_start_date:
        return False
    return True