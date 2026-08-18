import rail

def do_format_logs():
    log_artifacts = []
    log_records = []

    projectlogs = rail.result("create_project_log")

    if projectlogs:
        if isinstance(projectlogs, list):
            log_artifacts.extend(projectlogs)
        else:
            log_artifacts.append(projectlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    project_codes = list(map(lambda x: {
        'projectcode': x['properties'].get('projectcode', '')
        }, log_records))

    final_data = list({f"{value['projectcode']}": value for value in project_codes}.values())

    #pylint: disable=cell-var-from-loop
    for item in final_data:
        project_logs = list(
            filter(lambda x: x['properties'].get('projectcode', '') == item['projectcode'], log_records))
        if len(project_logs) > 0:
            first = project_logs[0]
            final_log_records.append({
                'projectcode': first['properties']['projectcode'],
                'projectname(code)': first['properties']['projectname(code)'],
                'projectname(name)': first['properties']['projectname(name)'],
                'programcode': first['properties']['programcode'],
                'programname(code)': first['properties']['programname(code)'],
                'programname(name)': first['properties']['programname(name)'],
                'clientname': first['properties']['clientname'],
                'clientcode': first['properties']['clientcode'],
                'projecttype': first['properties']['projecttype'],
                'details': ', '.join(list(map(lambda x: x['properties'].get('details'), project_logs))),
                'status': first['properties']['status'],
                'ecid': first['ecid'],
            })

    return final_log_records

def can_update_task(replicon_task_details, payload_task_details):
    status = payload_task_details['taskstatus'] == 'Open'
    return ((replicon_task_details['task_code'] != payload_task_details['taskcode']) or
            (replicon_task_details['task_start_date'] != payload_task_details['taskstartdate']) or
            (replicon_task_details['task_end_date'] != payload_task_details['taskenddate']) or
            (replicon_task_details['status'] != status))


def get_task_to_add_update_skip():
    current_task_in_project = rail.result('get_all_tasks_for_project')
    task_to_process = rail.load_all_records(rail.result("get_task_data_from_query"))

    if not task_to_process or not current_task_in_project:
        return {
        'tasks_to_add': task_to_process if not current_task_in_project else [],
        'tasks_to_update': [],
    }


    task_to_add = []
    task_to_update = []
    task_to_skip = []
    for task in task_to_process:
        task_details = rail.find_first_by_attr_and_get_attr(
            current_task_in_project, "task_name", task['taskcode'])
        if task_details:
            if can_update_task(task_details, task):
                task['uri'] = task_details['uri']
                if not task['taskstartdate'] and task_details['task_start_date']:
                    task['taskstartdate'] = task_details['task_start_date']
                if not task['taskenddate'] and task_details['task_end_date']:
                    task['taskenddate'] = task_details['task_end_date']
                task_to_update.append(task)
                continue
            task_to_skip.append({
                "task": task, "message": "No Change in the update"})
            continue
        task_to_add.append(task)

    return {
        'tasks_to_add': task_to_add if task_to_add else task_to_add,
        'tasks_to_update': task_to_update if task_to_update else task_to_update,
        "task_to_skip": task_to_skip
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

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_log_skipped_message():
    if get_task_state("is_enddate_less_than_today") =='success' :
        return 'Project is not created/updated since the manager is disabled and the end date has passed in Replicon'

    return 'Project is not created/updated due to the manager is not available in Replicon'
