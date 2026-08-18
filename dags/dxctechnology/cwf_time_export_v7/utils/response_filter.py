def get_last_time_export_details(response, dag_run):
    response = response.json()['d']['extensionFieldValues']

    if not response:
        return False

    data = list(filter(lambda x: x['name'] == dag_run.conf['oef_name'], map(lambda item: {
        'name': item['definition']['displayText'],
        'textValue': item['textValue']
    }, response)))

    return bool(data[0]['textValue'] == "Yes") if data else False

def get_oef_uris(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['name'] == 'GSAP_Payload_Processed', map(lambda item:{
        'name': item['displayText'],
        'uri': item['uri']
    }, response)))

def get_psa_oef_uris(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['name'] == 'PSA_Payload_Processed', map(lambda item:{
        'name': item['displayText'],
        'uri': item['uri']
    }, response)))


def get_psa_cost_centers(response):
    data = response['rows']
    if not data:
        return []

    psa_cost_center_list = list(filter(lambda x: x['parent'] == 'PSA Cost Center', map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "parent": row['cells'][1]['cellCollection'][0]['textValue'],
    }, data)))

    return str(list(map(lambda item: item['name'], psa_cost_center_list)))[1:-1]


def get_psa_orgs(response):
    data = response['rows']
    if not data:
        return []

    psa_org_units_list =  list(filter(lambda x:'PSA Org Unit' in (x['parent'], x['name']), map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "parent":  row['cells'][1]['cellCollection'][1]['textValue'] if len(
                row['cells'][1]['cellCollection']) > 1 else row['cells'][1]['cellCollection'][0]['textValue'],
    }, data)))

    return str(list(map(lambda item: item['name'], psa_org_units_list)))[1:-1]
