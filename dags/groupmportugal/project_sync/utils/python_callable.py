from datetime import datetime, timedelta, timezone
from airflow.models import Variable, DagRun
from rail import find_first_by_attr_and_get_attr, result, load_all_records, set_result, write_json_artifact

def get_unique_clients(dag_run):
    payload = dag_run.conf['payload']['array']
    unique_clients = []
    unique_advertiser = set()
    for record in payload:
        if not record['advertiser'] in unique_advertiser:
            unique_advertiser.add(record['advertiser'])
            unique_clients.append({
                'advertiser':record['advertiser'],
                'clienturi': find_first_by_attr_and_get_attr(result('get_all_clients'), 'name', record['advertiser'], 'uri', '')
            })
    return unique_clients

def if_project_details_not_present():
    project_details = result('get_project_details')
    return not project_details or not project_details.get('name', False)

def if_task_not_present(dag_run):
    return not bool(find_first_by_attr_and_get_attr(result('get_all_project_task'), "task.code", dag_run.conf['campaign_key'], "task.uri"))

def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):

    current_time = datetime.now(timezone.utc)
    lookup_timestamp_value = Variable.get(
        lookup_log_timestamp_var, default_var=None)

    query_execution_start_date = datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
        current_time - timedelta(hours=lookup_log_timestamp_hours))

    dag_runs = []
    execution_dates = []
    for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
        execution_dates.append(run.execution_date)
        dag_runs.append(run.id)
    if execution_dates:
        max_execution_date = max(execution_dates)
        Variable.set(lookup_log_timestamp_var,
                     (max_execution_date + timedelta(seconds=1)).isoformat())
    return dag_runs

def get_unique_log_artifacts_callable():
    logs = result("get_project_logs")
    project_logs = []
    for log in logs:
        project_logs.extend([log['project_log']])
    return list(set(project_logs))

def format_logs_callable():
    final_log_records = []
    logs = get_unique_log_artifacts_callable()
    for log in logs:
        final_log_records.extend(load_all_records(log))
    set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'error', final_log_records))))
    set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'success', final_log_records))))
    return write_json_artifact(final_log_records)
