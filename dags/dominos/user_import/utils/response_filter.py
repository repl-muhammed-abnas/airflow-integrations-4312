from rail import find_first_by_attr_and_get_attr


def get_supervisor_details(response, dag_run):
    get_userdata_supervisor = list(map(lambda item: {
        'username': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri']
    }, response['rows'])) if response['rows'] else []

    supervisor = list(filter(lambda x: x['loginname'] == dag_run.conf['supervisorid'],
                             get_userdata_supervisor)) if get_userdata_supervisor else []
    return {
        'name': supervisor[0]['username'] if supervisor else '',
        'uri': supervisor[0]['uri'] if supervisor else ''
    }


def is_assign_supervisorpermission(response):

    supervisor_permission = False
    if response:
        if not find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet'):
            supervisor_permission = True
    return supervisor_permission
