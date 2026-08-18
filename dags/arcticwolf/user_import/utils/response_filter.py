import rail


def get_location_list(response):
    groupdata = response['rows']
    return [{
        'name': data['cells'][0].get('textValue'),
        'uri': data['cells'][0].get('uri'),
        'fullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in data['cells'][1]['cellCollection']], '/')
    } for data in groupdata]


def get_supervisor_uri_and_status(response, dag_run):
    users_found = response['rows']
    supervisor = {}
    for user in users_found:
        if user['cells'][1]['textValue'] == dag_run.conf['supervisor']:
            supervisor = user
            break
    return {
        'uri': supervisor['cells'][0]['uri'] if supervisor else '',
        'status': supervisor['cells'][2]['textValue'] if supervisor else ''
    }


def get_supervisor_uri_and_status_assign_supervisor(response, dag_run):
    users_found = response['rows']
    matching_users = list(filter(
        lambda user: user['cells'][1]['textValue'] == dag_run.conf['supervisor'], users_found))
    return {
        'matchingusersfound': len(matching_users),
        'uri': matching_users[0]['cells'][0]['uri'] if matching_users else '',
        'status': matching_users[0]['cells'][2]['textValue'] if matching_users else ''
    }


def get_employeetype_list(response):
    employeetypes = response['rows']
    return [{
        'employeetypename': employeetype['cells'][0].get('textValue'),
        'employeetypeuri': employeetype['cells'][0].get('uri'),
        'employeetypefullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in employeetype['cells'][1]['cellCollection']], '|'),
        'employeetypelength': len([cell['textValue'] for cell in employeetype['cells'][1]['cellCollection']])
    } for employeetype in employeetypes]
