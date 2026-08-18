from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable, DagRun

def load_artiact_data(data):
    return rail.load_all_records(rail.result(data))

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

def map_user_details_with_feed_callable():
    feed_data = load_artiact_data("query_records_to_process")
    report_user_data = load_artiact_data("filter_required_users_from_report")
    final_data = []

    for record in feed_data:
        user_details = rail.find_first_by_attr_and_get_attr(
            report_user_data, "Employee_ID", record['empid'])
        final_data.append({
            **record,
            **{
                "user_uri": user_details['UserUri'],
            }
        })
    return final_data

def get_unique_log_artifacts_callable():
    logs = rail.result("get_project_logs")
    project_logs = []
    for log in logs:
        project_logs.extend([log['project_log'], log['exception_log']])
    return list(set(project_logs))

def do_format_logs():
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

def get_log_message_per_item(item,status,action,details):
    return {
            "employee_id" : item['empid'],
            "projectcode": item['projectcode'],
            "projectname": item['projectname'],
            "taskcode": item['taskcode'],
            "taskname": item['taskname'],
            'action': action,
            "details": details,
            "Status": status
        }

mandatory_fields = {
    "project_fields": {
        "empid": "empid",
        "projectcode": "projectcode",
        "taskname": "taskname"
    }
}

def get_invalid_logs_property_conf(item):
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields['project_fields']:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "employee_id" : item['empid'],
        "projectcode": item['projectcode'],
        "projectname": item['projectname'],
        "taskcode": item['taskcode'],
        "taskname": item['taskname'],
        'action': 'Add',
        "details": get_missing_field() + " not present in feed file",
        "Status": 'Skipped'
    }

def can_update_task(replicon_task_details, payload_task_details):
    return ((replicon_task_details['task_code'] != payload_task_details['taskcode']))

def get_task_to_add_update_skip():
    task_mapper_for_add = {}
    task_counter = {}
    current_task_in_project = rail.result('get_all_tasks_for_project') if bool(rail.result('get_all_tasks_for_project')) else []
    task_to_process = rail.load_all_records(rail.result("get_project_data_from_query"))

    task_to_add = []
    task_to_update = []
    add_tasks_to_log = []
    for task in task_to_process:
        task_details = rail.find_first_by_attr_and_get_attr(
            current_task_in_project, "task_code", task['taskcode'])
        if task_details:
            task['uri'] = task_details['uri']
            task_to_update.append(task)
            continue
        add_tasks_to_log.append(task)
        if task['taskcode'] in task_counter:
            task_counter[task['taskcode']] = task_counter[task['taskcode']] + 1
        else:
            task_counter[task['taskcode']] = 1
        if task_counter[task['taskcode']] == 1:
            task_mapper_for_add[task['taskcode']] = [task]
        else:
            task_mapper_for_add[task['taskcode']] = task_mapper_for_add[task['taskcode']] + [task]

    for taskuid, value in task_mapper_for_add.items():
        task_to_add.append({taskuid : value})
    return {
        'tasks_to_add': task_to_add,
        'tasks_to_update': task_to_update,
        'action': 'update'
    }

def map_task_success_error(task_id, action):
    res = []
    task_add_update_result = rail.result(task_id)
    task_list = rail.result("get_all_task_to_add_update")[f'tasks_to_{action}']

    if action == 'add' and rail.result("get_all_task_to_add_update")['action'] == 'update':
        for idx, task_res in enumerate(task_add_update_result):
            task_data = task_list[idx]
            for item in task_data.values():
                for data in item:
                    status = "Success"
                    msg = "Task and the User Assignment added Successfully"
                    if task_res.get("error"):
                        msg = ";".join([error.get('displayText')
                                    for error in task_res.get("error").get('notifications')])
                        status = "Error"
                    data['status'] = status
                    data['details'] = msg
                    res.append(data)
        return res

    for idx, task_res in enumerate(task_add_update_result):
        task_detail = task_list[idx]
        status = "Success"
        msg = "Task and the User Assignment added Successfully" if action == 'add' else \
        "Task and the User Assignment Updated Successfully"
        if task_res.get("error"):
            msg = ";".join([error.get('displayText')
                           for error in task_res.get("error").get('notifications')])
            status = "Error"
        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_list[idx])
    return res
