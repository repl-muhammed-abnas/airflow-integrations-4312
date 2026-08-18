import rail

from eisner_amper.project_import_internal_add_api.utils import request_payload

def map_task_success_error(dag_run):
    task_list = request_payload.get_data_to_process(dag_run)
    res = []
    for idx, task_res in enumerate(rail.result('create_task')):
        task_detail = task_list[idx]
        status = "Success"
        msg = f"Task Added Successfully"
        if task_res.get("error"):
            msg = ";".join([error.get('displayText')
                           for error in task_res.get("error").get('notifications')])
            status = "Error"
        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_list[idx])
    return res

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []

# pylint: disable=too-many-branches
def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []
    client_logs = dag_run.conf['client_logs']
    project_logs = dag_run.conf['project_logs']

    if client_logs:
        if isinstance(client_logs, list):
            log_artifacts.extend(client_logs)
        else:
            log_artifacts.append(client_logs)

    if project_logs:
        if isinstance(project_logs, list):
            log_artifacts.extend(project_logs)
        else:
            log_artifacts.append(project_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    error_record_count = 0
    success_record_count = 0
    exception_record_count = 0

    final_logs_records = []
    error_and_exception_records = []
    for log in log_records:
        if log['severity'].lower() == "error":
            error_record_count +=1
            error_and_exception_records.append({
                **{
                    'jobid': log['ecid'],
                    'details': log['message']
                },
                    **dict(log['properties'].items()),
            })
        if log['severity'].lower() == "success":
            success_record_count +=1
        if log['severity'].lower() == "exception":
            exception_record_count +=1
            error_and_exception_records.append({
                **{
                    'jobid': log['ecid'],
                    'details': log['message']
                },
                    **dict(log['properties'].items()),
            })
        final_logs_records.append({
        **{
            'jobid': log['ecid'],
            'details': log['message']
        },
            **dict(log['properties'].items()),
        })

    rail.set_result(key="error_record_count",val= error_record_count)
    rail.set_result(key="success_record_count",val= success_record_count)
    rail.set_result(key="exception_record_count",val= exception_record_count)
    rail.set_result(key="error_and_exception_records",val= error_and_exception_records)
    return  final_logs_records
