"""
ViaPlus User Sync - Python Callable Methods

Python callable methods for PythonOperator tasks.
"""
import itertools
import rail


def get_process_users_dag_ids(parallel_count):
    """Get all triggered DAG run IDs for gathering results."""
    active_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_active_users_{x+1}') if rail.result(
            f'process_active_users_{x+1}') else []), range(parallel_count)))))

    disable_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_disable_users_{x+1}') if rail.result(
            f'process_disable_users_{x+1}') else []), range(parallel_count)))))

    return active_users + disable_users


def do_format_logs(dag_run):
    """Format log records for CSV export."""
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf.get('userlogs')
    otherlogs = dag_run.conf.get('otherlogs')

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
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log.get('ecid', '')
        },
        **log.get('properties', {}),
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x.get('status') == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x.get('status') == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x.get('status') == 'Exception', final_log_records))))
    rail.set_result(key="total_record_count", val=dag_run.conf.get('total_records', 0))

    return final_log_records
