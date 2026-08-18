import rail
from datetime import datetime


def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def _is_rehire_after_or_equal_enddate(dag_run):
    """
    CR-V1.0 Branch A gate: when both 'rehire' and 'enddate' are present in the feed,
    only take the rehire path if the rehire date is on or after the enddate. Otherwise
    the record is logged as an exception and skipped.
    If 'enddate' is blank, rehire path proceeds unconditionally.
    """
    if not dag_run.conf.get('enddate'):
        return True
    if not dag_run.conf.get('rehire'):
        return False
    try:
        rehire_dt = datetime.strptime(dag_run.conf['rehire'], '%d/%m/%Y')
        end_dt = datetime.strptime(dag_run.conf['enddate'], '%d/%m/%Y')
    except (TypeError, ValueError):
        return False
    return rehire_dt >= end_dt

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
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
            'jobid': log['ecid'],
            'message': log['message']
        },
        **dict(log['properties'].items()),
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Skipped', final_log_records))))

    return final_log_records
