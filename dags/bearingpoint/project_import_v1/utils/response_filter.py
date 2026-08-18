import rail
from bearingpoint.project_import_v1.mapper.employee_type import EMPLOYEE_TYPE_MAPPER

def get_client_data_from_list_service(response,dag_run):
    if not response['rows']:
        return []

    response_data = list(filter(lambda item: item['code'] == dag_run.conf['client_code'],map(lambda row: {
        "name": row['cells'][3]['textValue'],
        "code": row['cells'][1]['textValue'],
        "uri": row['cells'][0]['uri']
    }, response['rows'])))

    return response_data[0] if response_data else []

def get_project_data_from_list_service(response):
    if not response['rows'][0]['cells']:
        return []

    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri']
    }, response['rows']))

def get_required_employee_types(response):
    if not response:
        return []

    return list(filter(lambda x: x['name'] not in EMPLOYEE_TYPE_MAPPER, map(lambda row: {
        "name": row['displayText'],
        "uri": row['uri']
    }, response)))

def get_project_team_member_uris(response):
    if not response:
        return []
    return list(filter(lambda x: x['name'] if x['name'] else None, map(lambda row: {
        "name": row['resource']['employeeTypeGroup'],
        "uri": row['resource']['uri']
    }, response)))

def get_groups_data(response):
    if not response:
        return []

    return list(map(lambda row: {
        "name": row['displayText'],
        "uri": row['uri']
    }, response))

def format_project_task_details(response):
    return list(map(lambda task: {
        "task_name": task['name'],
        "task_code": task['code'],
        "uri": task['uri'],
        "parent_task_name": task['parent'].get('task',{}).get('name') if task['parent'] else None,
        "parent_task_uri": task['parent'].get('task',{}).get('uri') if task['parent'] else None
    }, response))

def get_user_data_from_list_service(response,dag_run):
    employee_ids =[id for id in [dag_run.conf['ProjManagerId'],dag_run.conf['ProjPartnerId'],dag_run.conf['ClientRepresentative']] if id]
    if not response['rows']:
        return []

    all_users = list(map(lambda row: {
        "employee_id": row['cells'][1]['textValue'] if row['cells'][1]['dataType'] != "urn:replicon:list-type:null" else '',
        "uri": row['cells'][0]['uri']
    }, response['rows']))

    return {
        'project_users': list(filter(lambda item: item['employee_id'] in employee_ids, all_users)),
        'all_users': all_users
    }

def get_user_permissions_data(response):
    if not response:
        return []

    return list(map(lambda item:{
        'uri': item['user']['uri'],
        'permission': item['permissionSet']['displayText']
    },response))

def get_permission_set_data_from_list_service(response):
    if not response['rows'][0]['cells']:
        return []

    permission_set = list(map(lambda row: {
        "permission_name": row['cells'][0]['textValue']
    }, response['rows']))

    return [name['permission_name'] for name in permission_set]
