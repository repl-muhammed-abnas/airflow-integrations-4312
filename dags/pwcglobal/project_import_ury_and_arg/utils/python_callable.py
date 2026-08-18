from datetime import datetime
from ast import literal_eval
import rail

null = None


def can_update_task(replicon_task_details, payload_task_details):
    if payload_task_details['allowtimeentry'] == "Yes":
        if not replicon_task_details['enddate'] and payload_task_details['enddate']:
            return True
        if payload_task_details['enddate'] and replicon_task_details['enddate']:
            task_enddate = datetime.strptime(replicon_task_details['enddate'], "%m/%d/%Y").date()
            project_enddate = datetime.strptime(payload_task_details['enddate'], "%m/%d/%Y").date()
            if task_enddate != project_enddate:
                return True
    return ((replicon_task_details['task_code'] != payload_task_details['taskcode']) or \
            (payload_task_details['allowtimeentry'] == 'No'))

def get_task_to_add_update_skip():
    current_task_in_project = rail.result('get_all_tasks_for_project') if bool(rail.result('get_all_tasks_for_project')) else []
    task_to_process = rail.load_all_records(rail.result("get_project_data_from_query"))

    if not task_to_process or not current_task_in_project:
        if task_to_process:
            for task in task_to_process:
                task['taskname'] = task['taskname'].split("|")
        return {
        'tasks_to_add': task_to_process if not current_task_in_project else [],
        'tasks_to_update': [],
        'task_to_skip': []
    }

    task_to_add = []
    task_to_update = []
    task_to_skip= []
    for task in task_to_process:
        task['taskname'] = "|".join([name.strip() for name in task['taskname'].split("|")])  # added enhancement to handle extra space before and after the taskname.
        task_name = task['taskname']
        task['taskname'] = task['taskname'].split("|")
        task_details = rail.find_first_by_attr_and_get_attr(
            current_task_in_project, "full_task_name", task_name)
        if task_details:
            if can_update_task(task_details, task):
                task['uri'] = task_details['uri']
                task_to_update.append(task)
                continue
            task_to_skip.append({
                "task": task, "message": "No Change in the Task Update"})
            continue
        task_to_add.append(task)
    if task_to_add:
        task_to_add = sorted(task_to_add, key=lambda x: len(x['taskname']))
    if task_to_update:
        task_to_update = sorted(task_to_update, key=lambda x: len(x['taskname']))

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

def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def get_status(item, logstatus):
    status = 'status' if item.get('status') else 'Status'
    return item[status].lower() == logstatus

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    projectlogs = dag_run.conf['projectlogs']
    otherlogs = dag_run.conf['otherlogs']

    if projectlogs:
        if isinstance(projectlogs, list):
            log_artifacts.extend(projectlogs)
        elif isinstance(projectlogs, str) and projectlogs[0] == '[':
            projectlogs = literal_eval(projectlogs)
            log_artifacts.extend(projectlogs)
        else:
            log_artifacts.append(projectlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
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
        **{"ecid":log['ecid']},
        **dict(log['properties'].items()),
        **{
            'taskname': literal_eval(log['properties']['taskname']) if log['properties']['taskname'] and \
                isinstance(log['properties']['taskname'], str) and \
                log['properties']['taskname'][0] == '[' else log['properties']['taskname'] \
                if isinstance(log['properties']['taskname'], list) else ''
        }
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: get_status(x, 'skipped'), final_log_records ))))

    return  final_log_records

def get_current_date_time():
    return datetime.now().strftime("%d%m%YT%H%M%S")
