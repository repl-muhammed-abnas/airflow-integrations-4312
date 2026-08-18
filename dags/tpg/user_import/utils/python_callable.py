# pylint: disable=unused-variable,too-many-statements,too-many-branches
from datetime import datetime
from ast import literal_eval
import rail

def get_current_date_time():
    return datetime.now().strftime("%d%m%YT%H%M%S")

def get_ref_file_name(filepath):
    return filepath + "/" + rail.result('list_reference_files')[filepath][0]['name']

def get_today():
    return datetime.now().strftime("%m/%d/%Y")

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
