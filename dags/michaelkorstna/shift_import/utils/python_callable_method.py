import rail
import json


null = None


def get_csv_files_list(result_task_id, input_file_path):
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


def get_non_csv_files_list(result_task_id, input_file_path):
    if not result_task_id or not input_file_path:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")
    data = rail.result(result_task_id)

    if not data:
        return []

    return list(filter(lambda x: '.csv' not in x['filename'], list(map(lambda item: {
        'filename': item['name'].lower(),
        'size': item['size'],
        'path': input_file_path + '/' + item['name']
    }, data[input_file_path])))) if data[input_file_path] else []


def do_format_logs():
    master_log = json.loads(rail.result('load_master_log'))

    log_artifacts = rail.result(
        'gather_logs') if rail.result('gather_logs') else []

    def load_records(log_artifact):
        try:
            logs = rail.load_all_records(log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)

            if each_log_records:
                master_log.extend(each_log_records)

    return list(map(lambda x: {
        **{k: v for k, v in x['properties'].items() if k != 'email'},
        **{
            'jobid': x['ecid']
        }}, master_log))
