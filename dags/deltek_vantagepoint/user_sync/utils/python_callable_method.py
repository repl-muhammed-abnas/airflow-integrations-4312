import itertools
import math
import rail


def getChunks(arrayof_obj):
    chunk_size = 50
    chunks = [arrayof_obj[i:i + chunk_size]
              for i in range(0, len(arrayof_obj), chunk_size)]
    return chunks


def getFilterExpression(employeeId):
    return {
        "leftExpression": {
            "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
        },
        "operatorUri": "urn:replicon:filter-operator:text-search",
        "rightExpression": {
            "value": {
                "text": employeeId
            }
        }
    }


def joinFilter(leftExpression, rightExpression, operatorUri):
    return {
        "leftExpression": leftExpression,
        "operatorUri": operatorUri,
        "rightExpression": rightExpression
    }


def combineLeaves(leaves):
    if not leaves:
        return None
    if len(leaves) == 1:
        return leaves[0]
    midpoint = math.ceil(len(leaves) / 2)
    return joinFilter(combineLeaves(leaves[:midpoint]), combineLeaves(leaves[midpoint:]), "urn:replicon:filter-operator:or")


def get_chunk_request(loginname_list, columnUris):
    leaves = []
    for loginname in loginname_list:
        filterExpression = getFilterExpression(loginname)
        leaves.append(filterExpression)
    finalFilterExpression = combineLeaves(leaves)
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": columnUris,
        "sort": [],
        "filterExpression": finalFilterExpression
    }


def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_users_to_search():
    users_loginname = set()
    all_users = rail.result('get_all_users_from_vp')
    webhook_user = rail.result('get_user_details_for_webhook')
    if all_users:
        for user in all_users:
            users_loginname.add(user['Employee'])
            users_loginname.add(user['Supervisor'])
    elif webhook_user:
        users_loginname.add(webhook_user[0]['Employee'])
        users_loginname.add(webhook_user[0]['Supervisor'])
    return list(users_loginname)


def get_userlist_request(config):
    userlist_request = []
    users_chunk = getChunks(get_users_to_search())
    columnuris = [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:login-name',
            'urn:replicon:user-list-column:employee-id',
            'urn:replicon:user-list-column:enabled',
            'urn:replicon:user-list-column:supervisor',
            'urn:replicon:user-list-column:timesheet-period'
    ]
    for group in config.groups:
        columnuris.append(group['columnuri'])
    for chunk in users_chunk:
        chunk_request = get_chunk_request(chunk, columnuris)
        userlist_request.append(chunk_request)
    return userlist_request

def get_dynamic_groups_value(x, groups):
    group_values = {}
    for index, group in enumerate(groups):
        group_values[group['type']] = x['cells'][6 + index]['uri'] if 'uri' in x['cells'][6 + index] else None
    return group_values

def get_user_data_from_list(response, config):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    return list(map(lambda x: {
        'uri': x['cells'][0]['uri'],
        'loginname': x['cells'][1]['textValue'] if 'textValue' in x['cells'][1] else None,
        'status': x['cells'][3]['textValue'] if 'textValue' in x['cells'][3] else None,
        'supervisor': x['cells'][4]['uri'] if 'uri' in x['cells'][4] else None,
        'timesheetperiod': x['cells'][5]['textValue'] if 'textValue' in x['cells'][5] else None,
        **get_dynamic_groups_value(x, config.groups)
    } if x else None, flatten_rows))

def get_exception_logs(dag_run, config):
    message = "".join(dag_run.conf['exception_messages']) or ''
    for oef in dag_run.conf['oefs']:
        if oef['id'] in [
            'laborcategory',
            'laborcodelevel1',
            'laborcodelevel2',
            'laborcodelevel3',
            'laborcodelevel4',
            'laborcodelevel5'
        ] and oef['input'] and not oef['value']:
            message += f"{oef['name']} not assigned since value not found in Replicon. "
    for group in config.groups:
        input_key, value_key = (group['type'] + 'name'), group['type']
        if dag_run.conf.get(input_key) and not dag_run.conf.get(value_key):
            message += f"{group['name']} not assigned since {dag_run.conf.get(input_key)} not found in Replicon. "
    should_assign_supervisor = dag_run.conf['supervisoruri'] and (dag_run.conf['supervisoruri'] != (
                dag_run.conf['currentdetails'] and dag_run.conf['currentdetails'].get('supervisor'))) and (not dag_run.conf[
                'is_user_own_supervisor'])
    if should_assign_supervisor and not dag_run.conf['is_supervisor_enabled']:
        message += "Supervisor not assigned since it is disabled in replicon. "

    return message
