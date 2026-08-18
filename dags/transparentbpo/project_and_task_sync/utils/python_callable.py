import rail
import json


def serialize_level_2_tasks(level_2_tasks_mapper, dag_run):
    # Filter and ensure the data is serializable
    filtered_tasks = list(filter(
        lambda x: x["Allowed"] == "Yes" and x["task_level_2"] != dag_run.conf['projectName'],
        level_2_tasks_mapper
    ))
    return json.dumps(filtered_tasks)

def get_error_message():
    error_message = json.dumps(rail.render_template("{{get_error_message()}}")).lower()
    known_errors = [
        "timed out connecting to server",
        "503 service unavailable",
        "failed to open tcp connection",
        "connection reset by peer",
        "504 gateway timeout",
        "419: unexpected token",
        "server broke connection"
    ]

    for known_error in known_errors:
        if known_error in error_message:
            return False
    return True

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    project_log_artifacts = dag_run.conf['project_sync_logs']

    if project_log_artifacts:
        if isinstance(project_log_artifacts, list):
            log_artifacts.extend(project_log_artifacts)
        else:
            log_artifacts.append(project_log_artifacts)


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

    return final_log_records