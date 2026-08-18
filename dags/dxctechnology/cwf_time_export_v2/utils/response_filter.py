def get_last_time_export_details(response, dag_run):
    response = response.json()['d']['extensionFieldValues']

    if not response:
        return False

    data = list(filter(lambda x: x['name'] == dag_run.conf['oef_name'], map(lambda item: {
        'name': item['definition']['displayText'],
        'textValue': item['textValue']
    }, response)))

    return bool(data[0]['textValue'] == "Yes") if data else False
