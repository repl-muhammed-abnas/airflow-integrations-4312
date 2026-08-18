import rail

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
    for log in log_records:
        if log['severity'].lower() == "error":
            error_record_count +=1
        if log['severity'].lower() == "success":
            success_record_count +=1
        if log['severity'].lower() == "exception":
            exception_record_count +=1
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
    return  final_logs_records
