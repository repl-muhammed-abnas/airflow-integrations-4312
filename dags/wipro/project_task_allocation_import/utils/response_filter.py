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
