def map_existing_project_tasks(response):
    if response:
        return [{
            'task_name': x['name'],
            'task_code': x['code'],
            'uri': x['uri'],

        } for x in response]
    return [{
        'task_name': 'nil',
        'task_code': 'nil',
        'uri': 'nil'
    }]
