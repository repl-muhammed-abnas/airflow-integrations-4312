def get_specific_project_uri(response, dag_run):
    return list(filter(lambda x: x['cells'][1]['textValue'] == dag_run.conf['item']['projectcode'], response.json()['d']['rows']))


def map_custom_field_groups(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == "Project", data))


def map_registration_udf_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == "Registration", data))
