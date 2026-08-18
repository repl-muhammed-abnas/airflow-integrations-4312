import rail
from rail import load_all_records
from daimlertrucks.liquidplanner_time_entry_sync.utils import custom_methods


null = None


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_timesheets_to_submit_logs():
    log_artifacts = []

    if rail.result('gather_timesheets_to_submit_logs_from_put_entries'):
        log_artifacts.append(rail.result(
            'gather_timesheets_to_submit_logs_from_put_entries'))

    if rail.result('gather_timesheets_to_submit_logs_from_delete_entries'):
        log_artifacts.extend(rail.result(
            'gather_timesheets_to_submit_logs_from_delete_entries'))

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


def do_format_time_entry_import_logs():
    log_artifacts = []

    if rail.result('create_time_entry_import_log'):
        log_artifacts.append(rail.result('create_time_entry_import_log'))

    if rail.result('gather_time_entry_import_logs_from_users'):
        log_artifacts.extend(rail.result(
            'gather_time_entry_import_logs_from_users'))

    if rail.result('gather_user_not_found_logs_from_users'):
        log_artifacts.extend(rail.result(
            'gather_user_not_found_logs_from_users'))

    if rail.result('gather_user_disabled_logs_from_users'):
        log_artifacts.extend(rail.result(
            'gather_user_disabled_logs_from_users'))

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


def get_timesheeturi_status():
    statuslist = list(filter(lambda x: x['timesheeturi'] == rail.result('foreach_query_list_75_76')['timesheeturi'], map(lambda item: {
        'timesheeturi': item['timesheeturi'],
        'status': item['status'],
    }, custom_methods.get_data_from_document(rail.result('create_collection_create_list_from_csv_74')))))
    return statuslist[0]['status']


def get_current_dag_runs():
    dag_run_list = []
    if rail.result('insert_to_deleting_exisitng_time_enteries_dag_run_list'):
        dag_run_list.extend(rail.result(
            'insert_to_deleting_exisitng_time_enteries_dag_run_list')['value'])

    if rail.result('insert_to_put_time_entries_dag_run_list'):
        dag_run_list.extend(rail.result(
            'insert_to_put_time_entries_dag_run_list')['value'])
    return dag_run_list
