def get_data_from_list_service(response):
    if not response['rows']:
        return []

    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri']
    }, response['rows']))[0]

def format_project_task_details(response):
    return list(map(lambda task: {
        "task_name": task['name'],
        "task_code": task['code'],
        "uri": task['uri']
    }, response))

def get_all_costcenters(response):
    if not response:
        return []
    return list( map(lambda item: {
        'name': item['displayText'],
        'uri': item['uri']
    }, response))

def get_required_costcenters(dag_run):
    data = list(filter(lambda item: item['name'] == '', dag_run.conf['cost_centers_data']))

    return [item['uri'] for item in data]

def map_resource_assignment_list(response):
    return [resource['resource']['uri'] for resource in response]
