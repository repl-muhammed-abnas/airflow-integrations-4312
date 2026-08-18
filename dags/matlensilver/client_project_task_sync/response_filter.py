import rail


def client_filter(response, clientid):
    data = response.json()['d']
    return list(filter(lambda x: x['code'] == clientid, map(lambda row: {
        'code': row['cells'][0]['textValue'] if row['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else None,
        'textValue': row['cells'][1]['textValue'] if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        'uri': row['cells'][2]['uri']
    }, data['rows'])))


def get_filtered_output_user(response, projectleader):
    data = response.json()['d']
    exactname = (', ').join(reversed(projectleader.split(' ')))
    return list(filter(lambda x: x['name'] == exactname, map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][1]['textValue'] if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][2]['textValue'],
    }, data['rows'])))


def get_filtered_tasks(response):
    data = response.json()['d']
    return list(map(lambda d: {
        "taskcode": d['task']['code'],
        "taskname": d['task']['name'],
        "uri": d['task']['uri'],
    }, data)) if response.json()['d'] else []


def get_filtered_resources(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "user": row['resource']['user']['displayText'],
        "uri": row['resource']['uri'],
    }, data)) if response.json()['d'] else []


def get_filtered_resource_data(response, personid):
    data = response.json()['d']
    return list(filter(lambda x: x['employeeid'] == personid and x['status'] == 'True', map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][1]['textValue'] if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][2]['textValue'],
    }, data['rows'])))


def get_filtered_tag_uri(response):
    return {
        'open_uri':  rail.find_first_by_attr_and_get_attr(response.json()['d']['tags'], 'name', 'Active', 'uri'),
        'close_uri': rail.find_first_by_attr_and_get_attr(response.json()['d']['tags'], 'name', 'Closed', 'uri')
    }


def get_filtered_oef_values(response):
    data = response.json()['d']
    return list(map(lambda row: {
        'displayText': row['definition']['displayText']
    }, data)) if response.json()['d'] else []
