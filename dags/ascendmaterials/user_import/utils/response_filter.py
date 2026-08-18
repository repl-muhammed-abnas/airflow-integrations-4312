
def get_supervisor(response, dag_run):
    users_found = response['rows']
    matching_user = list(filter(
        lambda user: user['cells'][1]['textValue'] == dag_run.conf['supervisorloginname'], users_found))
    return {
        'uri': matching_user[0]['cells'][0]['uri'] if matching_user else '',
        'status': matching_user[0]['cells'][2]['textValue'] if matching_user else ''
    }

def get_manager(response, dag_run):
    users_found = response['rows']
    matching_user = list(filter(
        lambda user: user['cells'][1]['textValue'] == dag_run.conf['manager'], users_found))
    return {
        'uri': matching_user[0]['cells'][0]['uri'] if matching_user else '',
        'status': matching_user[0]['cells'][2]['textValue'] if matching_user else ''
    }
