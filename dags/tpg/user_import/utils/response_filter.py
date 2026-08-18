import rail

GROUPS_DELIMITER = '|'

null_urn = "urn:replicon:list-type:null"

def get_value(item, index, pluck_key):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else ""

def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return GROUPS_DELIMITER.join([item['textValue'] for item in full_path_list])

def groups_filter(response):
    if not response['rows']:
        return []

    resp = list(map(lambda data: {
        "name": get_value(data['cells'], 0, 'textValue'),
        "uri": get_value(data['cells'], 1, 'cellCollection')[-1]['uri'],
        "full_path": get_full_path(data['cells'][1]['cellCollection'])
    }, response['rows']))
    return resp

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))

def filter_group_data(res):
    return list(
        map(lambda data:
            {
                'name': get_value(data['cells'], 0, 'textValue'),
                'uri': get_value(data['cells'], 0, 'uri'),
                'code': get_value(data['cells'], 1, 'textValue'),
            }, res['rows'])
    )

def map_supervisor_list_data(response):
    if not response:
        return None
    return {
        'name': response[0]['userDetails']['displayText'],
        'loginname': response[0]['securityConfiguration']['loginName'],
        'uri':  response[0]['userDetails']['uri'],
        'status':  response[0]['userDetails']['isEnabled']
    }

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {}) if data[0].get(key, {}) else {}

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'employeeType', 'division']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

def get_filtered_user_data(response):
    return [] if response == [None] else response

def is_assign_supervisorpermission(response):

    supervisor_permission = False
    if response:
        if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet'):
            supervisor_permission = True
    return supervisor_permission

def get_division_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def filter_divisions_data(response):
    if not response['rows']:
        return []
    return list(map(lambda division: {
        "name": get_division_value(division, 0, 'textValue'),
        "uri":  get_division_value(division, 1, 'uri'),
    }, response['rows']))
