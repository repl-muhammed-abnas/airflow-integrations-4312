def get_project_type_oef_uri(response):
    return list(filter(lambda x: x['name'] == 'Project Export Type', map(lambda item:{
        'name': item['displayText'],
        'uri': item['uri']
    }, response)))
