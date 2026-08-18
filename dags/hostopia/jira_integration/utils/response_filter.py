import rail

object_list_type_uri = 'urn:replicon:list-type:object'


def filter_programs_data(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['name'] == rail.result('board_check')[0]['location']['projectName'], list(map(lambda item: {
        "name": item['name'],
        "uri": item['uri']
    }, response))))


def program_manager_check(response):
    response = response.json()['d']
    if not response:
        return []

    return lambda x: x['programManager'], response


None_urn = "urn:replicon:list-type:None"


def project_list(response):
    response = response.json()['d']['rows']
    if not response:
        return []

    return list(map(lambda item: {
        "Projectname": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != None_urn else None,
        "Projecturi": item['cells'][0]['uri'] if item['cells'][0]['dataType'] != None_urn else None,
        "Projectstatus": item['cells'][1]['textValue'] if item['cells'][1]['dataType'] != None_urn else None,
        "Projectstartdate": item['cells'][2]['textValue'] if item['cells'][2]['dataType'] != None_urn else None,
        "Projectenddate": item['cells'][3]['textValue'] if item['cells'][3]['dataType'] != None_urn else None
    }, response))


def map_user_list(response):
    return list(map(lambda x: {
        "name": rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', object_list_type_uri, 'textValue'),
        "uri": rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', object_list_type_uri, 'uri'),
    }, response['rows'])) if response['rows'] else None


def get_users_data(response):
    response = response.json()['d']['rows']
    if not response:
        return []

    return list(map(lambda item: {
        "name": item['cells'][0]['textValue'],
        "uri": item['cells'][0]['uri'],
    }, response))


def map_existing_project_tasks(response):
    if response:
        return [{
            'name': x['name'],
            'uri': x['uri'],

        } for x in response]
    return [{
        'name': 'nil',
        'uri': 'nil'
    }]


def get_user_column(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['name'] == 'AccountId', map(lambda item: {
        'name': item['displayText'],
        'uri': item['uri']
    }, response[1]['columns'])))


def get_user_list_filters(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['name'] == 'AccountId', map(lambda item: {
        'name': item['name'],
        'uri': item['uri']
    }, response)))

def get_task_uris(response):
    response = response.json()['d']
    if not response:
        return []

    return list(map(lambda item: {
        'uri': item['uri']
    }, response))
