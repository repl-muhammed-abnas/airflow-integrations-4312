import json
from datetime import datetime, timedelta
from ast import literal_eval
import hashlib
import pendulum
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from rail import find_first_by_attr_and_get_attr, result, load_all_records, write_json_artifact, set_result

DATE_FORMAT = "%Y-%m-%d"

def check_timeoff_type_assigned_to_user(dag_run):
    user_timeoff_policy_data = result("get_user_info")["timeOffTypePolicySummary"]["policiesByTimeOffType"]
    return find_first_by_attr_and_get_attr(user_timeoff_policy_data, "timeOffType.name", dag_run.conf["timeoff_name"])

def do_get_last_run_date(config):
    current_time = pendulum.now()
    last_run_date = Variable.get(config.last_run_date_var_name, default_var="")
    Variable.set(config.last_run_date_var_name, current_time.strftime(DATE_FORMAT))
    return last_run_date

def bamboohr_timeoff_data():
    return [json.loads(result('get_conf_payload'))] if result('get_conf_payload') else result('get_users_timeoff')

def get_timeoff_data():
    last_run_date = result('get_last_run_date')
    timeoff_data = bamboohr_timeoff_data()
    if not bool(timeoff_data and timeoff_data[0]):
        return []
    timeoff_data = list(filter(lambda records: records['status']['status'] == 'approved' or records['status']['status'] == 'canceled' or records['status']['status'] == 'superceded', timeoff_data))
    if last_run_date:
        last_run_date = datetime.strptime(last_run_date, DATE_FORMAT)
        # last_24_hour = pendulum.now() - timedelta(hours=24)
        timeoff_data = list(filter(lambda records: datetime.strptime(records['status']['lastChanged'], DATE_FORMAT).date() >= last_run_date.date(), timeoff_data))
    return timeoff_data


def load_records(log_artifact):
    return load_all_records(log_artifact)

def do_format_logs(main_log, child_log):
    log_artifacts = []
    log_records = []

    if main_log:
        if isinstance(main_log, list):
            log_artifacts.extend(main_log)
        elif isinstance(main_log, str) and main_log[0] == '[':
            main_log = literal_eval(main_log)
            log_artifacts.extend(main_log)
        else:
            log_artifacts.append(main_log)

    if child_log:
        if isinstance(child_log, list):
            log_artifacts.extend(child_log)
        elif isinstance(child_log, str) and child_log[0] == '[':
            child_log = literal_eval(child_log)
            log_artifacts.extend(child_log)
        else:
            log_artifacts.append(child_log)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **log['properties'],
        'ecid': log['ecid']
        }, log_records))
    
    set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))

    return  write_json_artifact(final_log_records)

