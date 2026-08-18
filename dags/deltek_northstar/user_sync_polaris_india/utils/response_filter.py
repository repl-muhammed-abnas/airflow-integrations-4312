import rail

GROUPS_DELIMITER = '|'

null_urn = "urn:replicon:list-type:null"
DATE_FORMAT = "%m/%d/%Y"

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

def filter_group_data(res):
    return list(
        map(lambda data:
            {
                'name': get_value(data['cells'], 0, 'textValue'),
                'uri': get_value(data['cells'], 0, 'uri'),
                'code': get_value(data['cells'], 1, 'textValue'),
            }, res['rows'])
    )

def get_filtered_user_data(response):
    return [] if response == [None] else response

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {}) if data[0].get(key, {}) else {}

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'employeeType', 'division', 'costCenter', 'serviceCenter']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

def filter_timeoff_types(response):
    return list(map(lambda row:
        {
            "timeoff_name": row["cells"][0]["textValue"],
            "status": row["cells"][2]["textValue"],
            "timeoff_uri": row["cells"][0]["uri"]
        }, response["rows"]))

def filter_cost_rate_response(response):
    rows = response['document']['rows']
    resp = []
    if not rows:
        return resp
    resp = rows[0]['row']['data']
    return resp

def map_supervisor_list_data(response):
    if not response:
        return None
    return {
        'firstname': response[0]['userDetails']['firstName'],
        'email': response[0]['userDetails']['emailAddress'],
        'name': response[0]['userDetails']['displayText'],
        'loginname': response[0]['securityConfiguration']['loginName'],
        'uri':  response[0]['userDetails']['uri'],
        'status':  response[0]['userDetails']['isEnabled']
    }

def is_assign_supervisorpermission(response):
    supervisor_permission = False
    if response:
        if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet'):
            supervisor_permission = True
    return supervisor_permission

def get_employee_type_name(employee_type):
    emp_type = ""
    if employee_type == "P":
        emp_type = "Part Time"
    if employee_type == "R":
        emp_type = "Regular"
    if employee_type == "T":
        emp_type = "Temporary"
    return emp_type

def get_available_timeoff_types(response, dag_run, timeoff_type_mapper):
    employee_type = get_employee_type_name(dag_run.conf['employee_type'])
    timeoff_types = rail.find_first_by_attr_and_get_attr(timeoff_type_mapper, "employee_type", employee_type, "timeoff_types", [])
    all_timeoff_types = {timeoff_type['name']: timeoff_type['uri'] for timeoff_type in response}
    available_in_instance = []
    not_available_in_instance = []
    for timeoff_type in timeoff_types:
        if timeoff_type in all_timeoff_types:
            available_in_instance.append({
                "name":timeoff_type,
                "uri":all_timeoff_types[timeoff_type]
                }
            )
        else:
            not_available_in_instance.append(timeoff_type)
    return {
        'available_in_instance': available_in_instance,
        'not_available_in_instance': not_available_in_instance
    }
