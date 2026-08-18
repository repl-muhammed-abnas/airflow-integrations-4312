from datetime import datetime
from functools import lru_cache
from collections import defaultdict
from itertools import chain
from ast import literal_eval
import uuid
import rail
from alvarezandmarsalholdings.enterprise_project_import_v4.utils.request_payload import MANDATORY_FIELDS, get_updated_task_details

null = None
EXISTING_DATE_FORMAT = "%m/%d/%Y"
PAYLOAD_DATE_FORMAT = "%Y-%m-%d"
TASK_ADD_UPDATE_BATCH_SIZE = 200
RESOURCE_BATCH_SIZE = 200

def can_update_task(replicon_task_details, payload_task_details):

    replicon_allow_time_entry = replicon_task_details.get('allow_time_entry')
    payload_allow_time_entry = payload_task_details.get('allow_time_entry')
    billing_resp = payload_task_details.get('billing_responsible')
    payload_task_status = payload_task_details.get('task_status')

    task_startdate = replicon_task_details.get('startdate')
    task_enddate = replicon_task_details.get('enddate')
    payload_startdate = payload_task_details.get('startdate')
    payload_enddate = payload_task_details.get('enddate')

    # Compare start or end dates if only one exists
    if (not task_startdate and payload_startdate) or (not task_enddate and payload_enddate):
        return True

    # Compare both start dates
    if task_startdate and payload_startdate:
        task_date = datetime.strptime(task_startdate, EXISTING_DATE_FORMAT).date()
        payload_date = datetime.strptime(payload_startdate, PAYLOAD_DATE_FORMAT).date()
        if task_date != payload_date:
            return True

    # Compare both end dates
    if task_enddate and payload_enddate:
        task_date = datetime.strptime(task_enddate, EXISTING_DATE_FORMAT).date()
        payload_date = datetime.strptime(payload_enddate, PAYLOAD_DATE_FORMAT).date()
        if task_date != payload_date:
            return True

    # Check Allow Time Entry
    if (payload_allow_time_entry == 'X' and replicon_allow_time_entry == 'Yes') or \
       (payload_allow_time_entry not in ['X'] and replicon_allow_time_entry == 'No'):
        return True

    # Compare billing responsibility
    if replicon_task_details.get('billing_resp') != billing_resp:
        return True

    # Check closed state vs. blocked flag
    is_closed = replicon_task_details.get('isclosed')
    if (is_closed and payload_task_status in ['10', 10]) or (not is_closed and payload_task_status not in ['10', 10]):
        return True

    # Check task name
    return replicon_task_details.get('task_name') != payload_task_details.get('taskname')


# SAP/CPI sends a two-level structure: the project (L1) and its project elements (L2).
# An L2 element carries ParentProject = the project code, so it becomes a level 1 task in
# Replicon. Anything sent below that is outside the agreed structure and is reported instead
# of being created. We don't build a new collection for these tasks because we already have
# their details here; duplicating the data would only slow down the performance.

def get_tasks_by_level(payload_tasks, current_project_tasks, projectcode):
    """
    Classifies incoming tasks into the single supported task level, orphans, tasks whose
    parents exist only in Replicon, and tasks sent below the supported L1-L2 structure.
    """

    # Prepare lookup sets
    existing_task_codes = set(current_project_tasks.keys())
    incoming_task_codes = set(task['taskcode'] for task in payload_tasks)

    # Adding project code in incoming task codes as matching project code will be that task level1
    incoming_task_codes.add(projectcode)

    all_known_codes = existing_task_codes.union(incoming_task_codes)

    # Build a lookup dictionary for incoming tasks
    incoming_lookup = {task['taskcode']: task for task in payload_tasks}

    # Prepare lists for categorization
    level1 = []
    orphan_tasks = []
    parent_in_system = []
    unsupported_level_tasks = []
    projectcode = projectcode.strip()

    for task in payload_tasks:
        parent_code = (task.get('parent_task') or '').strip()

        if parent_code and parent_code not in all_known_codes:
            # Orphan: Parent is not known
            orphan_tasks.append(task)
        elif parent_code in existing_task_codes and parent_code not in incoming_task_codes:
            # Parent exists in system only
            parent_in_system.append(task)
        elif parent_code and parent_code == projectcode:
            # Parent is the project itself: the L2 element, held as a level 1 task
            level1.append(task)
        elif parent_code in incoming_lookup:
            # Parent is itself an element in this payload, so the task sits below L2
            unsupported_level_tasks.append(task)
        else:
            orphan_tasks.append(task)

    # A row without a ParentProject describes the project itself, not a task under it
    remaining_orphans = [
        orphan for orphan in orphan_tasks if (orphan.get('parent_task') or '').strip()
    ]

    return {
        'task_level1': level1,
        'orphan_tasks': remaining_orphans,
        'parent_in_system': parent_in_system,
        'unsupported_level_tasks': unsupported_level_tasks
    }


def get_staffing_entries(section, field):
    """
    Normalises a staffing section into a list of codes.

    SAP/CPI sends StaffingCostCenter and StaffedEmployee as JSON arrays and always includes
    both sections even when one is empty, so an absent, null or empty section yields [].
    A single object is still accepted so an older-style payload does not fail hard.
    """
    if not section:
        return []
    entries = section if isinstance(section, list) else [section]
    return [
        str(entry[field]).strip()
        for entry in entries
        if isinstance(entry, dict) and entry.get(field) and str(entry[field]).strip()
    ]


def get_formatted_payload_tasks(dag_run):
    """
    Parses and formats the incoming task payload, validates mandatory fields,
    classifies tasks into levels, and filters out invalid (orphan) data.
    """

    existing_tasks = rail.result('get_all_tasks_for_project')

    task_details = rail.load_json_artifact(dag_run.conf['A_EnterpriseProjectElementType'])

    projectcode = dag_run.conf['Project']

    raw_tasks = []
    missing_fields = []
    resources = defaultdict(list)
    cost_center_resource = defaultdict(list)
    billing_responsibles = set()
    task_code_and_name = {}

    def get_missing_mandatory_fields(item, message):
        skip = False
        for key, value in MANDATORY_FIELDS['task_fields'].items():
            if not item.get(key):
                message.append(f"Mandatory Field {value} is not present in payload")
                skip = True
        return skip

    for item in task_details:
        message = []
        task_code = item['ProjectElement']
        task_name = item['ProjectElementDescription']

        if get_missing_mandatory_fields(item, message):
            missing_fields.append({
                'taskname': task_name,
                'taskcode': f'task_not_found_in_payload_{uuid.uuid4()}' if not task_code else task_code,
                'message': rail.smartjoin_by_delim(message, ";")
            })
            continue

        formatted_task = {
            'taskname': task_name,
            'taskcode': task_code,
            'startdate': item['PlannedStartDate'],
            'enddate': item['PlannedEndDate'],
            'task_status': item['ProcessingStatus'],
            'allow_time_entry': item['EntProjTimeRecgIsBlkd'],
            'billing_responsible': item['YY1_BillingRespProjEl_PTD'],
            'parent_task': item['ParentProject']
        }

        task_code_and_name[task_code] = formatted_task
        raw_tasks.append(formatted_task)

        if item.get('YY1_BillingRespProjEl_PTD'):
            billing_responsibles.add((item['YY1_BillingRespProjEl_PTD'], task_name, task_code))

        cost_centers = get_staffing_entries(item.get('StaffingCostCenter'), 'CostCenter')
        if cost_centers:
            cost_center_resource[task_code].extend(cost_centers)

        staffed_employees = get_staffing_entries(item.get('StaffedEmployee'), 'StaffedEmployee')
        if staffed_employees:
            resources[task_code].extend(staffed_employees)

    # Classify tasks
    task_levels = get_tasks_by_level(raw_tasks, existing_tasks, projectcode)
    unsupported_level_tasks = task_levels.pop('unsupported_level_tasks')

    # Final list of valid tasks (excluding orphans)
    valid_tasks = (
        task_levels['task_level1'] +
        task_levels['parent_in_system']
    )
    valid_task_codes = {task['taskcode'] for task in valid_tasks}

    # Filter resources and cost centers to only include valid tasks
    filtered_resources = {
        code: val for code, val in resources.items() if code in valid_task_codes
    }
    filtered_cost_center_resource = {
        code: val for code, val in cost_center_resource.items() if code in valid_task_codes
    }

    task_code_and_name = {
        code: item for code, item in task_code_and_name.items() if code in valid_task_codes
    }

    billing_responsibles = [
        resp for resp in billing_responsibles if resp[2] in valid_task_codes
    ]

    unsupported_hierarchy = [
        {
            'taskcode': task['taskcode'],
            'taskname': task['taskname'],
            'message': f"Task {task['taskcode']} is below the supported L1-L2 structure "
                       f"and was not processed"
        } for task in unsupported_level_tasks
    ]

    formatted_payload = [{
        **task_levels,
        'resources': filtered_resources,
        'cost_center_resource': filtered_cost_center_resource,
        'missing_mandatory_fields': missing_fields,
        'unsupported_hierarchy': unsupported_hierarchy,
        'billing_responsibles': billing_responsibles,
        'task_code_and_name': task_code_and_name
    }]
    return rail.write_json_artifact(formatted_payload)

def get_batch_records(records, batch_size=400):
    """
    Divide a list of dictionaries into chunks of specified size.
    
    Args:
        records: List of dictionaries to be chunked
        batch_size: Size of each chunk (default: 400)
    
    Returns:
        List of lists, where each inner list contains batch_size dictionaries
    """
    batches = []
    for i in range(0, len(records), batch_size):
        batches.append(records[i:i + batch_size])
    return batches

def get_task_to_add_update_skip(level, task_to_process, project_code):
    current_task_in_project = get_updated_task_details(level)

    if not task_to_process or not current_task_in_project:
        return {
        'tasks_to_add': get_batch_records(task_to_process, TASK_ADD_UPDATE_BATCH_SIZE) if not current_task_in_project else [],
        'tasks_to_update': [],
        'task_to_skip': []
    }

    task_to_add = []
    task_to_update = []
    task_to_skip = []
    for task in task_to_process:
        taskcode = task['taskcode']
        task_name = task['taskname']
        
        # In case of child task first check if parent task is present
        if task.get('parent_task') and task['parent_task'] != project_code:
            parent_task = task['parent_task']
            parent_task_details = current_task_in_project.get(parent_task, '')
            if not parent_task_details:
                task_to_skip.append({
                    "task": task, "message": f"Parent task {parent_task} is not present for child task {taskcode}"})
                continue
            task_name = f"{parent_task_details['task_name']}|{task_name}"
        # For the task level1 or level2 first check with task_code
        task_details = current_task_in_project.get(taskcode, '')
        
        if task_details:
            if can_update_task(task_details, task):
                task['uri'] = task_details['uri']
                task_to_update.append(task)
                continue
            task_to_skip.append({
                "task": task, "message": "No change is received for the task"})
            continue
        task_to_add.append(task)

    return {
        'tasks_to_add': get_batch_records(task_to_add, TASK_ADD_UPDATE_BATCH_SIZE),
        'tasks_to_update': get_batch_records(task_to_update, TASK_ADD_UPDATE_BATCH_SIZE),
        'task_to_skip': task_to_skip
    }

def map_task_success_error(all_task_name, task_id, action, _type):
    task_add_update_result = rail.result(task_id)['response']
    task_list = rail.result(all_task_name)[_type]
    task_list = list(chain.from_iterable(task_list))
    res = []
    for idx, task_res in enumerate(task_add_update_result):
        task_detail = task_list[idx]
        status = "Success"
        msg = f"Task {action}ed Successfully"
        if task_res.get("error"):
            msg = ";".join(list(set([error.get('displayText')
                           for error in task_res.get("error").get('notifications')])))
            if msg in ('A task with this name already exists.', 'The specified Task already exists.'):
                status = "Exception"
                msg = "Task was skipped since the specified Task name already exists with the different task code."
            else:
                status = "Error"
        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_list[idx])
    return res

@lru_cache(maxsize=16)
def get_all_data_from_json_artifact(artifact_name):
    return rail.load_all_records(artifact_name)

def get_project_team_members_uris(dag_run, instance):

    resp = {
        'resource_uris': [],
        'log_messages': []
    }
    
    payload_data = get_all_data_from_json_artifact(rail.result('format_payload_tasks'))[0]
    task_names = payload_data.get('task_code_and_name', {})
    
    # Prepare once to reduce repeated calls
    costcenter_data = get_all_data_from_json_artifact(dag_run.conf['get_all_costcenters'])
    user_data = get_all_data_from_json_artifact(dag_run.conf['get_all_users_data'])

    unique_costcenters = set()
    unique_employees = set()

    costcenter_mapping = defaultdict(list)
    employee_mapping = defaultdict(list)

    for task_code, cost_centers in payload_data.get('cost_center_resource', {}).items():
        if task_code in task_names:
            for cc in cost_centers:
                unique_costcenters.add(cc)
                costcenter_mapping[cc].append(task_code)

    for task_code, employees in payload_data.get('resources', {}).items():
        if task_code in task_names:
            for emp_id in employees:
                unique_employees.add(emp_id)
                employee_mapping[emp_id].append(task_code)

    messages = []

    for cc in unique_costcenters:
        uri = rail.find_first_by_attr_and_get_attr(costcenter_data, 'code', cc, 'uri')
        if uri:
            resp['resource_uris'].append(uri)
        else:
            for task_code in costcenter_mapping[cc]:
                messages.append({
                    'task_name': task_names.get(task_code, {}).get('taskname'),
                    'task_code': task_code,
                    'message': f"CostCenter {cc} is not present in Replicon"
                })

    for emp_id in unique_employees:
        uri = rail.find_first_by_attr_and_get_attr(user_data, 'employeeid', emp_id, 'uri')
        if uri:
            resp['resource_uris'].append(uri)
        else:
            for task_code in employee_mapping[emp_id]:
                messages.append({
                    'task_name': task_names.get(task_code, {}).get('taskname'),
                    'task_code': task_code,
                    'message': f"StaffedEmployee {emp_id} is disabled or not present in Replicon"
                })

    if messages:
        resp['log_messages'] = messages
    resp['resource_uris'] = get_batch_records(resp['resource_uris'], batch_size=RESOURCE_BATCH_SIZE)
    return resp

# NEW: Cache the URI lookup maps per dag_run
@lru_cache(maxsize=4)
def build_uri_lookup_maps(get_all_users_data_key, get_all_costcenters_key):
    """Build lookup dictionaries once for all users and cost centers.
    Uses hashable keys for caching."""
    
    # Load data (already cached by get_all_data_from_json_artifact)
    all_users = get_all_data_from_json_artifact(get_all_users_data_key)
    all_costcenters = get_all_data_from_json_artifact(get_all_costcenters_key)
    
    # Build O(1) lookup dictionaries - this is the expensive part we want to cache
    user_uri_map = {
        user['employeeid']: user['uri'] 
        for user in all_users 
        if 'employeeid' in user and 'uri' in user
    }
    
    cc_uri_map = {
        cc['code']: cc['uri'] 
        for cc in all_costcenters 
        if 'code' in cc and 'uri' in cc
    }
    
    return user_uri_map, cc_uri_map

def get_add_remove_resource(dag_run, instance):
    response = {
        "resource_to_add": [],
        "resource_to_remove": set()
    }

    payload = get_all_data_from_json_artifact(rail.result('format_payload_tasks'))[0]
    assigned_tasks = rail.result('get_final_descendant_task_details')

    # Build lookup maps ONCE (cached across calls)
    user_uri_map, cc_uri_map = build_uri_lookup_maps(
        dag_run.conf['get_all_users_data'],
        dag_run.conf['get_all_costcenters']
    )

    # Convert payload to lookup maps
    received_users_map = {
        task: set(users) for task, users in payload["resources"].items()
    }
    received_cc_map = {
        task: set(ccs) for task, ccs in payload["cost_center_resource"].items()
    }

    # ============= ADD LOGIC =============
    resources_to_add = defaultdict(list)
    
    # Process all tasks at once - Add users
    for task_code, received_users in received_users_map.items():
        task_uri = assigned_tasks.get(task_code, {}).get("uri")
        if not task_uri:
            continue
        
        # Direct O(1) dictionary lookups
        uris = [
            {'uri': user_uri_map[emp_id]} 
            for emp_id in received_users
            if emp_id in user_uri_map
        ]
        
        if uris:
            resources_to_add[task_uri].extend(uris)
            response['resource_to_remove'].add(task_uri)

    # Process all tasks at once - Add cost centers
    for task_code, received_ccs in received_cc_map.items():
        task_uri = assigned_tasks.get(task_code, {}).get("uri")
        if not task_uri:
            continue
        
        # Direct O(1) dictionary lookups
        uris = [
            {'uri': cc_uri_map[cc_code]} 
            for cc_code in received_ccs
            if cc_code in cc_uri_map
        ]
        
        if uris:
            resources_to_add[task_uri].extend(uris)
            response['resource_to_remove'].add(task_uri)

    resource_to_add_list = [
        {'task_uri': task_uri, 'record_id': idx, 'uris_artifact': rail.write_json_artifact(uris)}
        for idx, (task_uri, uris) in enumerate(resources_to_add.items())
    ]
    response['resource_to_add'] = resource_to_add_list

    response['resource_to_remove'] = list(response['resource_to_remove'])

    return response

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

    def get_log_status(project_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], project_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"
    
    def get_log_details(project_logs):
        message = list(set(map(lambda x: x['properties'].get('details'), project_logs)))
        return "; ".join([m for m in message if m != "No change is received for the task"]) if len(message) > 1 else "; ".join(message)

    final_log_records = []

    project_task_codes = list(map(lambda x: {
        'project_task_code': f"{x['properties'].get('projectcode', '')}|{x['properties'].get('taskcode', '')}"
        }, log_records))

    final_data = list({f"{value['project_task_code']}": value for value in project_task_codes}.values())

    #pylint: disable=cell-var-from-loop
    for item in final_data:
        project_logs = list(
            filter(lambda x: 
                   (x['properties'].get('projectcode', '') == item['project_task_code'].split('|')[0]) and 
                   (x['properties'].get('taskcode', '') == item['project_task_code'].split('|')[1]), log_records))
        if len(project_logs) > 0:
            first = project_logs[0]
            final_log_records.append({
                'projectcode': first['properties']['projectcode'],
                'projectname': first['properties']['projectname'],
                'taskcode': '' if first['properties']['taskcode'].startswith('task_not_found_in_payload_') else first['properties']['taskcode'],
                'taskname': first['properties']['taskname'],
                'action': first['properties']['action'],
                'status': get_log_status(project_logs),
                "details":  get_log_details(project_logs),
                'ecid': first['ecid'],
            })

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: get_status(x, 'skipped'), final_log_records ))))

    return  final_log_records

def get_permissions_to_assign(dag_run, config):
    billing_users = get_all_data_from_json_artifact(rail.result('format_payload_tasks'))[0]['billing_responsibles']
    users_data = rail.result('get_billing_responsible_users_data')
    message = []
    permissions_to_add = []

    for user_detail in billing_users:
        user = rail.find_first_by_attr_and_get_attr(
            users_data, 'employeeid', user_detail[0]
        )
        if not user:
            message.append({
                'task_name': user_detail[1],
                'task_code': user_detail[2],
                'message': f"BillingResponsible user {user_detail[0]} is disabled or not present in Replicon"
            })
            continue
        if not bool(rail.find_first_by_attr_and_get_attr(user['permission_sets'],
            "uri", dag_run.conf['supervisor_permissionuri'])):
            permissions_to_add.append({
                "userUri": user['uri'],
                "permissionSetUri": dag_run.conf['supervisor_permissionuri']
            })
        if not bool(rail.find_first_by_attr_and_get_attr(user['permission_sets'],
            "uri", dag_run.conf['end_user_with_report_edit_permissionuri'])):
            permissions_to_add.append({
                "userUri": user['uri'],
                "permissionSetUri": dag_run.conf['end_user_with_report_edit_permissionuri']
            })
    return {
        'permissions_to_add': permissions_to_add,
        'log_details': message
    }

def get_project_manager_permission_to_assign(dag_run):
    permission_sets = rail.result('get_user_info_on_empid')['permission_sets']
    user_uri = rail.result('get_user_info_on_empid')['uri']
    permissions_to_add = []
    if not bool(rail.find_first_by_attr_and_get_attr(permission_sets,
            "uri", dag_run.conf['project_manager_permissionuri'])):
            permissions_to_add.append({
                "userUri": user_uri,
                "permissionSetUri": dag_run.conf['project_manager_permissionuri']
            })
    if not bool(rail.find_first_by_attr_and_get_attr(permission_sets,
            "uri", dag_run.conf['end_user_with_report_edit_permissionuri'])):
            permissions_to_add.append({
                "userUri": user_uri,
                "permissionSetUri": dag_run.conf['end_user_with_report_edit_permissionuri']
            })
    return permissions_to_add
    
def get_tasks_for_hierarchy_update():
    response = {
        "update": [],
        "msg": []
    }
    update_hierarchy = get_all_data_from_json_artifact(rail.result('format_payload_tasks'))[0]['task_code_and_name']
    existing_tasks = rail.result('get_final_descendant_task_details')
    for _, item in update_hierarchy.items():
        if (existing_tasks.get(item['taskcode']) and existing_tasks.get(item['parent_task'])) and \
            (item['parent_task'] != existing_tasks[item['taskcode']]['parent_task_code']):
            response['update'].append(
                {
                    'taskuri': existing_tasks[item['taskcode']]['uri'],
                    'targeturi': existing_tasks[item['parent_task']]['uri'],
                }
            )
            response['msg'].append({
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                'details': f"task hierarchy updated for task {item['taskcode']} under task {item['parent_task']}"
            })
    return response

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()
