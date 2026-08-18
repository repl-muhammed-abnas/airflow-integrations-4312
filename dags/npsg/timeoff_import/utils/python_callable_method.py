import rail

null = None


def get_timesheetstatus_entries():

    def load_records(log_artifact):
        try:
            logs = rail.load_all_records(log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []

    log_artifacts = []
    if rail.result('gather_timeoff_import_timesheetstatus_logs'):
        log_artifacts.extend(rail.result(
            'gather_timeoff_import_timesheetstatus_logs'))

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


def do_format_logs():

    def load_records(log_artifact):
        try:
            logs = rail.load_all_records(log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []

    log_artifacts = []
    if rail.result('create_timeoff_import_logs'):
        log_artifacts.append(rail.result('create_timeoff_import_logs'))

    if rail.result('gather_timeoff_import_child_logs'):
        log_artifacts.extend(rail.result('gather_timeoff_import_child_logs'))

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
