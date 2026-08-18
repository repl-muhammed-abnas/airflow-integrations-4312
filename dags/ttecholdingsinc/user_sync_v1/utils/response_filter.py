import json
import ast
import rail

null_urn = "urn:replicon:list-type:null"

GROUPS_DELIMITER = '|'


def get_value(item, index, pluck_key):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else ""


def filter_group_data(res):
    return list(
        map(lambda data:
            {
                'name': get_value(data['cells'], 0, 'textValue'),
                'uri': get_value(data['cells'], 0, 'uri'),
                'code': get_value(data['cells'], 1, 'textValue'),
            }, res['rows'])
    )


def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return GROUPS_DELIMITER.join([item['textValue'] for item in full_path_list])


def groups_filter(response):
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


def get_filtered_time_off_types(response):
    return list(map(lambda item: {
        "timeoff_type_name": item['displayText'],
        'timeoff_type_uri': item['uri'],
    }, response))


def get_filtered_user_data(response):
    return [] if response == [None] else response


def get_policy_to_assign(response):
    if not response:
        return None
    res = list(map(lambda item: {
        'description': 'effective',
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))


def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})


def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'employeeType', 'serviceCenter']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))


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
        permissions_to_add.append(dag_run.conf['manager_permissionset_uri'])

    if not end_user_permission:
        permissions_to_add.append(
            dag_run.conf['user_permissionset_uri'])

    return permissions_to_add

def map_assigned_policy_to_user(response):
    return list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:time-punch", response))
