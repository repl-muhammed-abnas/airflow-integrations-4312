import rail


def get_task_details(response):
    response = response.json()['d']

    return list(map(lambda item: {
        "taskcode": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', 'urn:replicon:list-type:string', 'textValue').lower(),
        "taskname": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', 'urn:replicon:list-type:object', 'textValue'),
        "uri": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', 'urn:replicon:list-type:object', 'uri')
    }, response['rows'])) if response['rows'] else []
