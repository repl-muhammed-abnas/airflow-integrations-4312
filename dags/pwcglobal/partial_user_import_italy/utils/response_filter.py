def search_user_response_filter(dag_run, response):
    return [{
            "user_uri": item['cells'][0]['uri'],
            "user_employee_id": item['cells'][1]['textValue'] if item['cells'][1]['dataType'] == 'urn:replicon:list-type:string' else ''
            }for item in response['rows'] if item['cells'][1]['textValue'] == dag_run.conf['user_party_id']] if response['rows'] else []


def get_all_div_response_filter(response):
    return [{
        'div_name': item['cells'][0]['textValue'] if item['cells'][0]['textValue'] else '',
        'div_code': item['cells'][1]['textValue'] if item['cells'][1]['dataType'] == "urn:replicon:list-type:string" else '',
        'div_uri': item['cells'][0]['uri']
    }for item in response['rows']] if response['rows'] else ''
