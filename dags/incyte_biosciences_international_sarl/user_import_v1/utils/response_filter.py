from datetime import datetime
import json
import ast
import rail

null_urn = "urn:replicon:list-type:null"

GROUPS_DELIMITER = '|'

DATE_FORMAT = "%d/%m/%Y"

def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_value(item, index, pluck_key):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else ""

def filter_group_data(res):
    return list(
        map(lambda data:
            {
                'name': get_value(data['cells'], 0, 'textValue'),
                'uri': get_value(data['cells'], 0, 'uri')
            }, res['rows'])
    )

def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return GROUPS_DELIMITER.join([item['textValue'] for item in full_path_list])

def filter_departments_data(response):
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "name": get_value(data['cells'], 0, 'textValue'),
        "uri": get_value(data['cells'], 1, 'cellCollection')[-1]['uri'],
        "full_path": get_full_path(data['cells'][1]['cellCollection'])
    }, response['rows']))

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))

def get_missing_permissions(response, dag_run):
    supervisor_permission = False
    end_user_permission = False
    permissions_to_add = []
    if response:
        supervisor_permission = len(
            [x for x in response if x['permissionSet']['name'] == 'Supervisor']) > 0
        end_user_permission = len([x for x in response if x['permissionSet']
                                  ['name'] == 'Basic User with Reports']) > 0

    if not supervisor_permission:
        permissions_to_add.append(dag_run.conf['supervisor_permission_uri'])

    if not end_user_permission:
        permissions_to_add.append(
            dag_run.conf['basic_user_permission_uri'])

    return permissions_to_add

def get_filtered_time_off_types(response):
    return list(map(lambda item: {
        "timeoff_type_name": item['displayText'],
        'timeoff_type_uri': item['uri'],
    }, response))

def get_policy_to_assign(response,dag_run):
    def get_effective_date():
        if dag_run.conf['action'] == 'rehire':
            return get_replicon_date(dag_run.conf['start_date'])
        return  get_replicon_date(dag_run.conf['todays_date'])

    if not response:
        return None
    res = list(map(lambda item: {
        'description': 'Added by integration: '+ (str(item['effectiveDate']) if dag_run.conf['action'] == 'add' else
            (str(dag_run.conf['start_date']) if dag_run.conf['action'] == 'rehire' else str(dag_run.conf['todays_date']))),
        'effectiveDate': item['effectiveDate'] if dag_run.conf['action'] == 'add' else get_effective_date(),
        'policySet': item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'"))) if dag_run.conf['action'] == 'add' else res

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'employeeType', 'costCenter', 'division', 'serviceCenter']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

def assigned_timeoffs_types_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        'timeoff_type_name': item['timeOffType']['displayText'],
        "timeoff_type_uri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    }, response['policiesByTimeOffType']))
