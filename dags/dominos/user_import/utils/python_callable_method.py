from rail import load_all_records, get_current_context


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs():
    dag_run = get_current_context()['dag_run']

    log_artifacts = dag_run.conf['child_log']

    log_records = []

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    return list(map(lambda x: {
        **dict(x['properties'].items()),
        **{
            'jobid': x['ecid']
        }}, log_records))
