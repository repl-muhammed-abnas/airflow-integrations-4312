from datetime import datetime
from pendulum import now
import rail

DATE_FORMAT = "%d.%m.%Y"
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

def validate_record(dag_run):
    if (dag_run.conf['currency_symbol_available_in_replicon']=='Yes' or 
        (dag_run.conf['currency_symbol_available_in_replicon']=='No' and not dag_run.conf['cost_rate'])) \
        and ((dag_run.conf['effective_date_for_activity_type'] and dag_run.conf['activity_type']) or \
        (dag_run.conf['effective_date_for_cost_rate'] and dag_run.conf['cost_rate'])):
        return True
    return False


def get_invalid_record_msg_child(dag_run):
    messages = []
    if dag_run.conf['currency_symbol_available_in_replicon']!='Yes':
        messages.append("Currency associated with Cost Rate not available in Replicon")
    if not dag_run.conf.get('activity_type') or not dag_run.conf.get('effective_date_for_activity_type'):
        messages.append("Activity Type/Valid-from date is not present in the feed file")
    if not dag_run.conf.get('cost_rate') or not dag_run.conf.get('effective_date_for_cost_rate'):
        messages.append("Cost Rate/Valid-from date is not present in the feed file")
    return " | ".join(messages) if messages else None

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = rail.result('gather_logs') if rail.result('gather_logs') else []
    otherlogs = rail.result('create_master_log') if rail.result('create_master_log') else []

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
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
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="total_record_count",val= rail.result('create_input_data_collection','length'))

    return final_log_records

def get_email_details_callable(timezone, log_file_path):
    _now = now(timezone)
    file_name = rail.render_template("{{ result('new_file_sensor') | file_base }}")
    return {
        "job_end_time" : (_now).isoformat(),
        "job_duration": (((_now - datetime.strptime(rail.result('log_start_time'), STANDARD_EMAIL_DATE_FORMAT)).seconds)//60),
        "log_timestamp": _now.strftime("%y%m%dT%H%M%S"),
        "email_timestamp": _now.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "log_file_name": f"Log_{file_name}.csv",
        "log_filepath": log_file_path
    }

def get_invalid_records_msg(item):
    if not item['employee_id']:
        return "Employee ID not present in the record"
    if not item['activity_type'] and not item['effective_date_for_activity_type'] and not item['cost_rate'] and not item['effective_date_for_cost_rate']:
        return "Activity Type, Effective Date for Activity Type, Cost Rate and Effective Date for Cost Rate are not present in the record"
    return ''
