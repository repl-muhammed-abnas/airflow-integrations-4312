import itertools


null = None


def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return null


def map_employeetypes(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    return list(map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'fullpath': ' | '.join([x['textValue'] for x in item['cells'][-1]['cellCollection']])
        if item['cells'][-1]['cellCollection'] else null,
        'uri': item['cells'][0]['uri']
    }, flatten_rows)) if flatten_rows else []


def map_user_details(response):
    return list(map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'employeeid': item['cells'][2].get('textValue')
    }, response['rows'])) if response['rows'] else []


def map_supervisor_list_data(response):
    return list(map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'employeeid': item['cells'][2].get('textValue'),
        'status': item['cells'][3]['textValue'],
        'enddate': item['cells'][4]['dateValue'] if item['cells'][4]['textValue'] else None,
        'startdate': item['cells'][5]['dateValue'] if item['cells'][5]['textValue'] else None,
    }, response['rows'])) if response['rows'] else []


def get_missing_permissions(response, dag_run):
    supervisor_permission = False
    end_user_permission = False
    permissions_to_add = []
    if response:
        supervisor_permission = len(
            [x for x in response if x['permissionSet']['name'] == dag_run.conf['supervisor_permission']]) > 0
        end_user_permission = len([x for x in response if x['permissionSet']
                                  ['name'] == dag_run.conf['supervisor_end_user_permission']]) > 0

    if not supervisor_permission:
        permissions_to_add.append(dag_run.conf['supervisor_permission_uri'])

    if not end_user_permission:
        permissions_to_add.append(
            dag_run.conf['supervisor_end_user_permission_uri'])

    return permissions_to_add
