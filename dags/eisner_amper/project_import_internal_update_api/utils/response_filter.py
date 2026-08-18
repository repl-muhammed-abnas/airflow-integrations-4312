import rail

def get_filtered_client_data(response,dag_run):
    if not response['rows']:
        return []
    return list(filter(lambda x: x['clientcode']==dag_run.conf['item']['ClientCode'], map( lambda item:{
        'clientname': item['cells'][1]['textValue'],
        'clientcode': item['cells'][2]['textValue'],
        'clienturi': item['cells'][0]['uri']
    }, response['rows'])))

def get_filtered_user_info(response, dag_run):
    data = response['rows']
    if not data:
        return []
    return list(filter(lambda x: x['employeeid'] == dag_run.conf['item']['ProjectManager'], map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "employeegrpfullpath": "|".join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection'])))
        if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        'division': row['cells'][5]['textValue'] if row['cells'][5]['dataType'] != 'urn:replicon:list-type:null' else None,
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][2]['textValue'] if row['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][3]['textValue'],
        "enddate": row['cells'][4]['textValue']
    }, data)))

def filter_cost_center_code(response, dag_run):
    return list(filter(lambda x: x['costcentercode'] == dag_run.conf['item']['ProjectCostCenterCode'], map( lambda item:{
        'costcentercode': item['cells'][1]['textValue'],
        'costcenteruri': item['cells'][0]['uri']
    },response['rows'])))

def filter_task_details(response):
    return list(filter(lambda x: x['code'] == rail.result('for_each_process_task')['TaskCode'], response))

def get_project_type_tag_uris(response):
    response = response.json()['d']
    if not response:
        return []

    return {
        '01': rail.find_first_by_attr_and_get_attr(response['tags'], "name", "01", "uri"),
        '02': rail.find_first_by_attr_and_get_attr(response['tags'], "name", "02", "uri"),
        '10': rail.find_first_by_attr_and_get_attr(response['tags'], "name", "10", "uri"),
        'NA': rail.find_first_by_attr_and_get_attr(response['tags'], "name", "NA", "uri")
    }

def get_project_profile_tag_uris(response):
    response = response.json()['d']
    if not response:
        return []

    return {
        'YP04': rail.find_first_by_attr_and_get_attr(response['tags'], "name", "YP04", "uri"),
        'YP02': rail.find_first_by_attr_and_get_attr(response['tags'], "name", "YP02", "uri")
    }
