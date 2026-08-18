
def get_task_list_data(response):
    response = response.json()['d']
    if not response:
        return []

    return list(map(lambda item: {
        "taskuri": item['cells'][0]['uri'],
        "taskname": item['cells'][0]['textValue'],
        "isenabled": item['cells'][1]['boolValue']
    }, response['rows']))


def get_all_team_members_data(response):
    response = response.json()['d']
    if not response:
        return []

    resource_uris = list(map(lambda item: {
        "uri": item['resource']['uri']
    }, response))

    return [x['uri'] for x in resource_uris]
