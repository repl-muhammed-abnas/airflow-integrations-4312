import rail
from four_liberty.task_import.utils import custom_methods
from rail import load_all_records

null = None


def get_reference_file(result_task_id, file_path):
    if not result_task_id or not file_path:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")
    data = rail.result(result_task_id)

    if not data:
        return []

    return list(filter(lambda x: x['filename'].split("_")[0] == rail.result('project_file_name'), list(map(lambda item: {
        'filename': item['name'],
        'size': item['size'],
        'path': file_path + '/' + item['name']
    }, data[file_path])))) if data[file_path] else []


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs():
    dag_run_conf = custom_methods.get_dag_run_conf()
    log_artifacts = []

    if dag_run_conf['task_import_logs']:
        log_artifacts.append(dag_run_conf['task_import_logs'])

    if dag_run_conf['task_create_logs']:
        log_artifacts.extend(dag_run_conf['task_create_logs'])

    if dag_run_conf['task_update_logs']:
        log_artifacts.extend(dag_run_conf['task_update_logs'])

    log_records = []
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)

            if each_log_records:
                log_records.extend(each_log_records)

    return list(map(lambda x: {
        **{k: v for k, v in x['properties'].items() if k != 'email'},
        **{
            'jobid': x['ecid']
        }}, log_records))
