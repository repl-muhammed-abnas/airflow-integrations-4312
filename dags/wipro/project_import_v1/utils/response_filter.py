def format_project_task_details(response):
    return list(map(lambda task: {
        "task_name": task['name'],
        "task_code": task['code'],
        "uri": task['uri']
    }, response))

def foreign_manager_details(response):
    return list(map(lambda item: {
        "user_name": item['cells'][0]['textValue'],
        "login_name": item['cells'][1]['textValue'],
        "uri": item['cells'][0]['uri']
    }, response['rows']))

def get_resource_start_date(response):
    task_details = list(map(lambda task: {
        "task_uri": task['task']['uri'],
        "assignment_start_date": task['allocationDateRange']['startDate']
    }, response['entries']))
    task_details.append({'employee_uri': response['resource']['uri']})

    return task_details

def get_project_type_oef_uri(response):
    return list(filter(lambda x: x['name'] == 'Project Export Type', map(lambda item:{
        'name': item['displayText'],
        'uri': item['uri']
    }, response)))
