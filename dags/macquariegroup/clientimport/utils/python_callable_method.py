import rail
from macquariegroup.clientimport.utils import custom_methods
from rail import load_all_records

null = None


def get_files_list(result_task_id, input_file_path):
    if not result_task_id or not input_file_path:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")
    data = rail.result(result_task_id)

    if not data:
        return []

    return list(filter(lambda x: '.csv' in x['filename'], list(map(lambda item: {
        'filename': item['name'].lower(),
        'size': item['size'],
        'path': input_file_path + '/' + item['name']
    }, data[input_file_path])))) if data[input_file_path] else []


def do_file_validations(result_task_id):

    def check_empty_files(files):
        for file in files:
            if file['size'] == 0:
                return True
        return False

    def check_incorrect_csv_files(files):
        for file in files:
            if file['filename'] != 'bu.csv' and file['filename'] != 'client.csv' and file['filename'] != 'locations.csv':
                return True
        return False

    if not result_task_id:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")

    data = rail.result(result_task_id)
    reason = null

    if not data:
        reason = 'there were no files found in the input folder'
    elif len(data) != 3:
        reason = 'the correct number of required Input files (3) are not present'
    elif check_empty_files(data):
        reason = 'file with 0 bytes is found in the Input folder'
    elif check_incorrect_csv_files(data):
        reason = 'of incorrect naming convention of Input file(s)'

    return reason if reason else 'valid'


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs():
    dag_run_conf = custom_methods.get_dag_run_conf()
    log_artifacts = []

    if dag_run_conf['client_import_logs']:
        log_artifacts.append(dag_run_conf['client_import_logs'])

    if dag_run_conf['client_add_logs']:
        log_artifacts.extend(dag_run_conf['client_add_logs'])

    if dag_run_conf['client_update_logs']:
        log_artifacts.extend(dag_run_conf['client_update_logs'])

    if dag_run_conf['client_disable_logs']:
        log_artifacts.extend(dag_run_conf['client_disable_logs'])

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


def filter_clients(dag_run):
    valid_clients = []
    invalid_clients = []
    for client in dag_run.conf['client_input']:
        if client['clientname'] is not null:
            valid_clients.append(client)
        else:
            invalid_clients.append(client)

    return {
        "valid_clients": valid_clients,
        "invalid_clients": invalid_clients
    }
