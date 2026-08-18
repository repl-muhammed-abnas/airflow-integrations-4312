from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable, DagRun

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
    logs = rail.result("gather_project_logs") or []
    project_logs = [] if logs else [rail.result("create_exception_log")]
    for log in logs:
        project_logs.extend([log['project_log'], log['exception_log']])
    return list(set(project_logs))

def format_logs_callable():
    final_log_records = []
    logs = get_unique_log_artifacts_callable()
    for log in logs:
        final_log_records.extend(rail.load_all_records(log))
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'exception', final_log_records))))
    return rail.write_json_artifact(final_log_records)

def can_update_task(replicon_task_details, payload_task_details):
    return ((replicon_task_details['task_code'] != payload_task_details['taskcode']))

def get_task_to_add_update_skip():
    current_task_in_project = rail.result('get_all_tasks_for_project') if bool(rail.result('get_all_tasks_for_project')) else []
    task_to_process = rail.load_all_records(rail.result("get_project_data_from_query"))

    if not task_to_process or not current_task_in_project:
        return {
        'tasks_to_add': task_to_process if not current_task_in_project else [],
        'tasks_to_update': [],
        'task_to_skip': []
    }

    task_to_add = []
    task_to_update = []
    task_to_skip= []
    for task in task_to_process:
        if len(task['taskname']) > 50:
            task_to_skip.append({
                "task": task, "message": "Task Name is more than 50 Char"})
            continue
        task_details = rail.find_first_by_attr_and_get_attr(
            current_task_in_project, "task_name", task['taskcode'])
        if task_details:
            if can_update_task(task_details, task):
                task['uri'] = task_details['uri']
                task_to_update.append(task)
                continue
            task_to_skip.append({
                "task": task, "message": "No Change in the Task Update"})
            continue
        task_to_add.append(task)

    return {
        'tasks_to_add': rail.load_all_records(rail.write_json_artifact(task_to_add)) if task_to_add else task_to_add,
        'tasks_to_update': rail.load_all_records(rail.write_json_artifact(task_to_update)) if task_to_update else task_to_update,
        'task_to_skip': rail.load_all_records(rail.write_json_artifact(task_to_skip)) if task_to_skip else task_to_skip
    }

def map_task_success_error(task_id, action, _type):
    task_add_update_result = rail.result(task_id)
    task_list = rail.result("get_all_task_to_add_update")[_type]
    res = []
    for idx, task_res in enumerate(task_add_update_result):
        task_detail = task_list[idx]
        status = "Success"
        msg = f"Task {action}ed Successfully"
        if task_res.get("error"):
            msg = ";".join([error.get('displayText')
                           for error in task_res.get("error").get('notifications')])
            status = "Error"
        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_list[idx])
    return res
