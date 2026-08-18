import itertools
from functools import lru_cache
from rail import result, find_first_by_attr_and_get_attr, load_all_records

null = None

def filter_all_costcenters_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    costcenter_info = list(filter(lambda item: item['isenabled'] in ['True', True], map(lambda row: {
        'name': row['cells'][0]['textValue'],
        'isenabled': row['cells'][1]['textValue'],
        'code': row['cells'][2]['textValue'],
        'uri': row['cells'][0]['uri'],
    }, flaten_rows)))
    return costcenter_info if costcenter_info else None

def filter_all_users_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    users_data = list(filter(lambda item: item['isenabled'] == True, map(lambda row: {
        'user_name': row['cells'][0]['textValue'],
        'loginname': row['cells'][1]['textValue'],
        'isenabled': row['cells'][3]['boolValue'],
        'employeeid': row['cells'][2].get('textValue', ''),
        'uri': row['cells'][0]['uri'],
    }, flaten_rows)))
    return users_data if users_data else None

def filter_all_tags_details(response):
    if not response['tags']:
        return []
    return list(filter(lambda item: item['isnabled'] == True, map(lambda row: {
        "name": row['name'],
        "uri": row['uri'],
        "isnabled": row['isEnabled'],
    }, response['tags'])))

def get_filtered_user_info(response):
    if not response:
        return {}
    return {
        "name": response[0]['userDetails']['displayText'],
        "isenabled": response[0]['userDetails']['isEnabled'],
        "uri": response[0]['userDetails']['uri'],
        "permission_sets": response[0]['permissionSets'],
    }

def format_project_task_details(response):
    tasks = []

    def get_oef_value(extension_field):
        resp = {
            "billing_control_catg": "",
            "billing_resp": ""
        }
        for field in extension_field:
            if field.get('definition', {}).get('displayText', '').strip() == 'Billing Control Category':
                resp['billing_control_catg'] = field['textValue']
            elif field.get('definition', {}).get('displayText', '').strip() == 'Billing Responsible':
                resp['billing_resp'] = field['textValue']
        return resp

    def add_child_taks(child_tasks, parent):
        for child_task in child_tasks:
            oef_details = get_oef_value(child_task['task']['extensionFieldValues'])
            child_task['task']['full_task_name'] = f"{parent}|{child_task['task']['name']}"
            child_task['task']['billing_resp'] = oef_details['billing_resp']
            child_task['task']['billing_control_catg'] = oef_details['billing_control_catg']
            tasks.append(child_task['task'])
            if child_task['childTasks']:
                add_child_taks(child_task['childTasks'], child_task['task']['name'])

    for item in response:
        oef_details = get_oef_value(item['task']['extensionFieldValues'])
        item['task']['full_task_name'] = f"{item['task']['name']}"
        item['task']['billing_resp'] = oef_details['billing_resp']
        item['task']['billing_control_catg'] = oef_details['billing_control_catg']
        tasks.append(item['task'])
        add_child_taks(item['childTasks'], item['task']['name'])

    tasks = list(map(lambda item: {
        "task_name": item['name'],
        "task_code": item['code'],
        "allow_time_entry": "Yes" if item['isTimeEntryAllowed'] else "No",
        "uri": item['uri'],
        "full_task_name": item['full_task_name'],
        'billing_resp': item['billing_resp'],
        'billing_control_catg': item['billing_control_catg'],
        'isclosed': item['isClosed'],
        'time_and_expense': item['timeAndExpenseEntryType'].get('displayText', ''),
        "startdate": str(item['timeEntryDateRange']['startDate']['month']) + '/' + 
        str(item['timeEntryDateRange']['startDate']['day']) + '/' + 
        str(item['timeEntryDateRange']['startDate']['year']) if item['timeEntryDateRange'] and \
            item['timeEntryDateRange']['startDate'] else null,
        "enddate": str(item['timeEntryDateRange']['endDate']['month']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['day']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['year']) if item['timeEntryDateRange'] and \
            item['timeEntryDateRange']['endDate'] else null
    }, tasks))

    return tasks

def create_task_uri_index(task_details):
    return {task['uri']: task['task_code'] for task in task_details}
 
def create_user_uri_index(users_data):
    return {user['uri']: user['employeeid'] for user in users_data}

def create_costcenter_uri_index(costcenters_data):
    return {cc['uri']: cc['code'] for cc in costcenters_data}

def get_resource_assigned(assignment, user_uri_index, costcenter_uri_index):
    resp = {"costcenters": [], "users": []}
    
    for item in assignment.get('assignments', []):
        resource_type = item['resource']['resourceType']['displayText']
        
        if resource_type == 'User':
            user = item['resource']['user']
            user['employeeid'] = user_uri_index.get(user['uri'], '')
            resp['users'].append(user)
            
        elif resource_type == 'Cost Center':
            costcenter = item['resource']['costCenter']
            costcenter['code'] = costcenter_uri_index.get(costcenter['uri'], '')
            resp['costcenters'].append(costcenter)
    
    return resp

def get_task_resource(response, dag_run):
    # Create indexes once
    get_all_task_details = result('get_all_task_details_for_project')
    task_uri_index = create_task_uri_index(get_all_task_details)
    
    all_users = load_all_records(dag_run.conf['get_all_users_data'])
    user_uri_index = create_user_uri_index(all_users)
    
    all_costcenters = load_all_records(dag_run.conf['get_all_costcenters'])
    costcenter_uri_index = create_costcenter_uri_index(all_costcenters)
    
    # Use indexes for O(1) lookups
    return list(map(lambda assignment: {
        'taskuri': assignment['taskUri'],
        'taskcode': task_uri_index.get(assignment['taskUri'], ''),
        'costcenters': get_resource_assigned(assignment, user_uri_index, costcenter_uri_index)['costcenters'],
        'users': get_resource_assigned(assignment, user_uri_index, costcenter_uri_index)['users']
    }, response))


        
def get_user_data_from_list(response):
    if not response:
        return []
    return list(filter(lambda item: item['isenabled'] == True, map(lambda resp: {
        "name": resp['userDetails']['displayText'],
        "employeeid": resp['userDetails']['employeeId'],
        "isenabled": resp['userDetails']['isEnabled'],
        "uri": resp['userDetails']['uri'],
        "permission_sets": resp['permissionSets'],
    }, response)))

def get_required_permission(response, config):
    resp = {
        'project_manager_permissionuri': '',
        'end_user_with_report_edit_permissionuri': '',
        'supervisor_permissionuri': ''
    }
    no_of_permissions = len(config.PERMISSIONS)
    for rec in response:
        if rec['displayText'] in config.PERMISSIONS:
            per_name = f"{rec['displayText'].replace(' ', '_').lower()}_permissionuri"
            resp[per_name] = rec['uri']
            no_of_permissions -= 1
        if no_of_permissions == 0:
            break
    return resp

def get_flatten_rows(response):
    return list(itertools.chain(*response))
