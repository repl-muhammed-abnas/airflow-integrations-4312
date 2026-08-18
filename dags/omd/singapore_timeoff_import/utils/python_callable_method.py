import rail
from rail import load_all_records

null = None


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_timeoff_import_logs():
    log_artifacts = []

    if rail.result('create_time_import_log'):
        log_artifacts.append(rail.result(
            'create_time_import_log'))

    if rail.result('gather_timeoff_import_logs_from_child'):
        log_artifacts.extend(rail.result(
            'gather_timeoff_import_logs_from_child'))

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
