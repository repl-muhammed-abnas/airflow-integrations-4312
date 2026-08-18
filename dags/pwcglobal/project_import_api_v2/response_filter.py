import rail


null = None
object_list_type_uri = 'urn:replicon:list-type:object'
string_list_type_uri = 'urn:replicon:list-type:string'
bool_list_type_uri = 'urn:replicon:list-type:bool'


def map_legal_entities_list(response):
    data = response.json()['d']
    legal_entities = []
    if data['rows']:
        filter_response_with_fullpath = list(filter(
            lambda x: x['cells'][1]['dataType'] == string_list_type_uri, data['rows']))
        legal_entities = list(map(lambda item: {
            "name": item['cells'][0]['textValue'],
            "uri": item['cells'][0]['uri'],
            "fullpath": item['cells'][1]['textValue'].lower()
        }, filter_response_with_fullpath))
    return legal_entities


def map_client_list(response):
    data = response.json()['d']
    client_list_mapped = []
    if data['rows']:
        client_list_mapped = list(map(lambda item: {
            "name": item['cells'][2]['textValue'],
            "code": item['cells'][1]['textValue'],
            "uri": item['cells'][0]['uri']
        }, data['rows']))
    return client_list_mapped


def map_company_codes_list(response):
    data = response.json()['d']
    print(data)
    mapped_company_codes = []
    if data['rows']:
        mapped_company_codes = list(map(lambda item: {
            "name": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', object_list_type_uri, 'textValue'),
            "uri": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', object_list_type_uri, 'uri'),
            "code": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', string_list_type_uri, 'textValue'),
            "fullpath": " / ".join([x['textValue'] for x in item['cells'][-1]['cellCollection']])
            if item['cells'][-1]['cellCollection'] else null,
            "fullpathuri": " / ".join([x['uri'] for x in item['cells'][-1]['cellCollection']])
            if item['cells'][-1]['cellCollection'] else null
        }, data['rows']))
    return mapped_company_codes


def map_user_list(response, item):
    return list(map(lambda x: {
        "name": rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', object_list_type_uri, 'textValue'),
        "uri": rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', object_list_type_uri, 'uri'),
        "status": rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', bool_list_type_uri, 'textValue'),
        "legal_entity_uri": item['PwCLegalEntity']['pwclegalentityuri'],
        "code": item['PwCLegalEntity']['PartyId'].lower(),
        "employee_id": rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', string_list_type_uri, 'textValue').lower()
    }, response['rows'])) if response['rows'] else null


def map_existing_project_tasks(response):
    if response:
        return [{
            'name': x['name'],
            'code': x['code'],
            'uri': x['uri']
        } for x in response]
    return [{
        'name': 'nil',
        'code': 'nil',
        'uri': 'nil'
    }]
