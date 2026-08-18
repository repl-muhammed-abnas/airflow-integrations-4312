import rail

def check_task_data(response, dag_run):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['Taskcode'] == dag_run.conf['item']['ProjName'], list(map(lambda item: {
        "Taskname": item['cells'][0]['textValue'],
        "Taskstatus": item['cells'][1]['textValue'],
        "Taskuri": item['cells'][0]['uri'],
        "Taskcode": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else None,
    }, response['rows']))))


def check_client_data(response, dag_run):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['clientcode'] == dag_run.conf['item']['CustomerCode'], list(map(lambda item: {
        "clienturi": item['cells'][1]['uri'],
        "clientcode": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else None,
    }, response['rows']))))


def get_uuid_oef_data(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == "UUID", list(map(lambda item: {
        "displayText": item['displayText'],
        "uri": item['uri'],
    }, data))))


def get_uuid_filter_data(response):
    data = response.json()['d']
    return list(filter(lambda x: x['name'] == "UUID", list(map(lambda item: {
        "name": item['name'],
        "uri": item['uri'],
    }, data))))


def get_timeentry_filter_data(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['uri'], list(map(lambda item: {
        "uri": item['cells'][0]['uri']
    }, response['rows']))))


def get_billable_nonbillable_timeentry_details(response):
    response = response.json()
    return list(map(lambda item: {
        "billable": rail.find_first_by_attr_and_get_attr(item['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:is-billable', 'value.bool'),
        "uri": item['uri'],
        "hours": item['interval']['hours']['hours'],
        "minutes": item['interval']['hours']['minutes']
    }, response['d']))
