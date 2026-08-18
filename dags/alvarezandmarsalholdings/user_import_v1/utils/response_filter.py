import rail


def get_supervisor_uri_and_status_assign_supervisor(response, dag_run):
    users_found = response['rows']
    matching_users = list(filter(
        lambda user: user['cells'][1]['textValue'] == dag_run.conf['supervisor'], users_found))
    return {
        'matchingusersfound': len(matching_users),
        'uri': matching_users[0]['cells'][0]['uri'] if matching_users else '',
        'status': matching_users[0]['cells'][2]['textValue'] if matching_users else ''
    }
