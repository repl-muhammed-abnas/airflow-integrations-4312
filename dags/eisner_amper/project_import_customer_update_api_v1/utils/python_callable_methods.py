import rail

def get_tasks_to_add_update():
    """Compare incoming tasks with existing tasks to determine add vs update operations"""
    current_tasks_in_project = rail.result('get_children_task_details') or []
    dag_run = rail.get_current_context()['dag_run']
    
    # Get incoming tasks from payload
    tasks_to_process = []
    if dag_run.conf['item']['WorkItemSet'] and dag_run.conf['item']['WorkItemSet']['WorkItem']:
        work_items = dag_run.conf['item']['WorkItemSet']['WorkItem']
        if isinstance(work_items, dict):
            tasks_to_process = [work_items]
        else:
            tasks_to_process = work_items
    
    tasks_to_add = []
    tasks_to_update = []
    tasks_with_validation_errors = []
    
    for task in tasks_to_process:
        task_code = task.get('TaskCode', '')
        task_name = task.get('TaskName', '')
        
        # Validate mandatory task fields
        if not task_code or not task_name:
            missing_fields = []
            if not task_name:
                missing_fields.append('TaskName is not present in payload')
            if not task_code:
                missing_fields.append('TaskCode is not present in payload')
            
            tasks_with_validation_errors.append({
                'TaskCode': task_code,
                'TaskName': task_name,
                'validation_error': rail.smartjoin_by_delim(missing_fields, ";")
            })
            continue
        
        # Find existing task by code
        existing_task = rail.find_first_by_attr_and_get_attr(
            current_tasks_in_project, "code", task_code)
        
        if existing_task:
            # Task exists - add to update list with existing task URI
            task['existing_task_uri'] = existing_task['uri']
            tasks_to_update.append(task)
        else:
            # Task doesn't exist - add to add list
            tasks_to_add.append(task)
    
    return {
        'tasks_to_add': tasks_to_add,
        'tasks_to_update': tasks_to_update,
        'tasks_with_validation_errors': tasks_with_validation_errors
    }

def map_add_task_results():
    """Map add task API results to log format"""
    add_result = rail.result("add_tasks")
    tasks_to_add = rail.result("get_tasks_to_add_update")['tasks_to_add']
    dag_run = rail.get_current_context()['dag_run']
    
    results = []
    for idx, task in enumerate(tasks_to_add):
        status = "Success"
        message = "Task added successfully"
        
        # Check if there was an error in the API response
        if add_result and isinstance(add_result, list) and len(add_result) > idx:
            task_result = add_result[idx]
            if task_result and task_result.get("error"):
                status = "Error"
                error_notifications = task_result.get("error", {}).get("notifications", [])
                message = ";".join([error.get('displayText', 'Unknown error') for error in error_notifications])
        
        result_entry = {
            'message': message,
            'severity': status,
            'clientcode': dag_run.conf['clientcode'],
            'projectcode': dag_run.conf['item']['ProjectCode'],
            'taskname': task['TaskName'],
            'taskcode': task['TaskCode'],
            'action': 'Add',
            'status': status,
        }
        results.append(result_entry)
    
    return results

def map_update_task_results():
    """Map update task API results to log format"""
    update_result = rail.result("update_tasks")
    tasks_to_update = rail.result("get_tasks_to_add_update")['tasks_to_update']
    dag_run = rail.get_current_context()['dag_run']
    
    results = []
    for idx, task in enumerate(tasks_to_update):
        status = "Success"
        message = "Task updated successfully"
        
        # Check if there was an error in the API response
        if update_result and isinstance(update_result, list) and len(update_result) > idx:
            task_result = update_result[idx]
            if task_result and task_result.get("error"):
                status = "Error"
                error_notifications = task_result.get("error", {}).get("notifications", [])
                message = ";".join([error.get('displayText', 'Unknown error') for error in error_notifications])
        
        result_entry = {
            'message': message,
            'severity': status,
            'clientcode': dag_run.conf['clientcode'],
            'projectcode': dag_run.conf['item']['ProjectCode'],
            'taskname': task['TaskName'],
            'taskcode': task['TaskCode'],
            'action': 'Update',
            'status': status,
        }
        results.append(result_entry)
    
    return results

def map_task_validation_errors():
    """Map task validation errors to log format"""
    validation_errors = rail.result("get_tasks_to_add_update")['tasks_with_validation_errors']
    dag_run = rail.get_current_context()['dag_run']
    
    results = []
    for task in validation_errors:
        result_entry = {
            'message': task['validation_error'],
            'severity': 'Exception',
            'clientcode': dag_run.conf['clientcode'],
            'projectcode': dag_run.conf['item']['ProjectCode'],
            'taskname': task['TaskName'],
            'taskcode': task['TaskCode'],
            'action': 'Validation',
            'status': 'Exception',
        }
        results.append(result_entry)
    
    return results

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []

# pylint: disable=too-many-branches
def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []
    client_logs = dag_run.conf['client_logs']
    project_logs = dag_run.conf['project_logs']

    if client_logs:
        if isinstance(client_logs, list):
            log_artifacts.extend(client_logs)
        else:
            log_artifacts.append(client_logs)

    if project_logs:
        if isinstance(project_logs, list):
            log_artifacts.extend(project_logs)
        else:
            log_artifacts.append(project_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    error_record_count = 0
    success_record_count = 0
    exception_record_count = 0

    final_logs_records = []
    error_and_exception_records = []
    for log in log_records:
        if log['severity'].lower() == "error":
            error_record_count +=1
            error_and_exception_records.append({
                **{
                    'jobid': log['ecid'],
                    'details': log['message']
                },
                    **dict(log['properties'].items()),
            })
        if log['severity'].lower() == "success":
            success_record_count +=1
        if log['severity'].lower() == "exception":
            exception_record_count +=1
            error_and_exception_records.append({
                **{
                    'jobid': log['ecid'],
                    'details': log['message']
                },
                    **dict(log['properties'].items()),
            })
        final_logs_records.append({
        **{
            'jobid': log['ecid'],
            'details': log['message']
        },
            **dict(log['properties'].items()),
        })

    rail.set_result(key="error_record_count",val= error_record_count)
    rail.set_result(key="success_record_count",val= success_record_count)
    rail.set_result(key="exception_record_count",val= exception_record_count)
    rail.set_result(key="error_and_exception_records",val= error_and_exception_records)
    return  final_logs_records

def map_task_success_error(task_id, action, task_type):
    """Map task operation results to log format"""
    task_result = rail.result(task_id)
    task_list = rail.result("get_task_to_add_update")[task_type]
    
    results = []
    for idx, task_res in enumerate(task_result):
        task_detail = task_list[idx].copy()  # Make a copy to avoid modifying original
        status = "Success"
        message = f"Task {action}ed successfully"
        
        if task_res.get("error"):
            message = ";".join([error.get('displayText', 'Unknown error') 
                              for error in task_res.get("error", {}).get('notifications', [])])
            status = "Error"
        
        # Update task detail with status and message
        task_detail['status'] = status
        task_detail['details'] = message
        
        # Create log entry format expected by the logging system
        log_entry = {
            'message': message,
            'severity': status,
            'clientcode': task_detail['clientcode'],
            'projectcode': task_detail['projectcode'],
            'taskname': task_detail['TaskName'],
            'taskcode': task_detail['TaskCode'],
            'action': action.title(),
            'status': status,
        }
        results.append(log_entry)
        
    return results
