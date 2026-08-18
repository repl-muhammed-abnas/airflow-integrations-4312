null_urn = "urn:replicon:list-type:null"


def get_all_enabled_company_codes(response):
    company_code_list = []
    data = response['rows']
    if data:
        company_code_list = list(map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "fullpath": " | ".join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))),
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
            "parenturi": row['cells'][1]['cellCollection'][0]['uri']
        }, data))
    return company_code_list


def map_non_contractor_employeetype_groups(response):
    data = response
    return list(filter(lambda x: 'contractor' not in x['name'].lower(), map(lambda item: {
        "name": item['displayText'],
        "uri": item['uri'],
        "status": "Yes"
    }, data)))


def get_filtered_client_data(response, dag_run):
    data = response['rows']
    return list(filter(lambda x: x['clientname'] == dag_run.conf['clientname'], map(lambda item: {
        "clientname": item['cells'][0]['textValue'],
        "clienturi": item['cells'][1]['uri'],
    }, data)))


def get_filtered_user_info(response, dag_run):
    data = response['rows']
    return list(filter(lambda x: x['employeeid'] == dag_run.conf['projectmanagerid'], map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "employeegrpfullpath": "|".join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection'])))
        if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        'division': row['cells'][5]['textValue'] if row['cells'][5]['dataType'] != 'urn:replicon:list-type:null' else None,
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][2]['textValue'] if row['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][3]['boolValue'],
        "enddate": row['cells'][4]['textValue']
    }, data)))

def get_date_from_json_date(json_date):
    if not json_date:
        return None
    return f"{json_date['day']}/{json_date['month']}/{json_date['year']}"

def get_user_details_filter(response, dag_run):
    user_data = response[0]
    return [{
        'name': user_data['userDetails']['displayText'],
        'employeegrpfullpath': dag_run.conf['perner_user']['current_employee_type_full_path'].replace(' / ', '|'),
        'division': dag_run.conf['perner_user']['current_company_code'],
        'status': user_data['userDetails']['isEnabled'],
        'enddate': get_date_from_json_date(user_data['userDetails']['employmentDateRange'].get('endDate', None)),
        'uri': dag_run.conf['perner_user']['user_uri'],
        'employeeid': dag_run.conf['projectmanagerid'],
        'other_user_info': user_data
    }]

def get_cost_centers(response):
    cost_center_list = []
    data = response['rows']
    if data:
        cost_center_list = list(filter(lambda x: x['parent'] != 'PSA Cost Center', map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
        }, data)))
    return cost_center_list


def get_filtered_data(response, dag_run):
    data = response.json()['d']['rows']
    return list(filter(lambda x: x['parentwbsname'] == dag_run.conf['wbsname'], map(lambda item: {
        "slug": item['cells'][0]['slug'],
        "textValue": item['cells'][0]['textValue'].split(' - ')[0].strip(),
        "uri": item['cells'][0]['uri'],
        "parentwbsname": item['cells'][1].get('textValue'),
    }, data))) if data else []


def get_value(item, index, key):
    if item['cells'][index]['dataType'] == null_urn:
        return None
    return item['cells'][index][key]


def get_full_path(item):
    return "/ ".join([x['textValue'] for x in item['cells'][1]['cellCollection']])


def all_task_response_filter(response):

    data = response.json()['d']
    if not data:
        return []

    return list(filter(lambda x: x['enabled'] == "True", map(lambda item: {
        "taskname": get_value(item, 0, 'textValue'),
        "uri": get_value(item, 0, 'uri'),
        "enabled": get_value(item, 3, 'textValue'),
        "task_fullpath": get_full_path(item) if item['cells'][1]['cellCollection'] else None,
        "parent_present": "True" if item['cells'][2]['dataType'] != null_urn else "False",
        "parent_task_name": get_value(item, 2, 'textValue'),
        "parent_task_uri": get_value(item, 2, 'uri'),
        "levels": len(item['cells'][1]['cellCollection']) if item['cells'][1]['cellCollection'] else 1,
        "code": get_value(item, 4, 'textValue'),
        "start_date": get_value(item, 5, 'textValue'),
        "end_date": get_value(item, 6, 'textValue')

    }, data['rows']))) if data['rows'] else []


def get_specific_task_details(response, dag_run):
    parent_full_path = "/ ".join(dag_run.conf["task_full_path"].split(
        "/ ")[:-1])
    data = response['rows']
    return list(
        filter(lambda x: x['taskname'] == dag_run.conf['parent'] and x['enabled'] == "True" and parent_full_path == x['task_fullpath'],
               map(lambda item: {
                   "taskname": get_value(item, 0, 'textValue'),
                   "uri": get_value(item, 0, 'uri'),
                   "enabled": get_value(item, 3, 'textValue'),
                   "task_fullpath": get_full_path(item) if item['cells'][1]['cellCollection'] else None,
               }, data)
               )
    ) if data else []


def filter_all_locations(response):
    location_list = []
    data = response['rows']
    if data:
        location_list = list(filter(lambda x: x['parent'] == 'Australia', map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
        }, data)))
    return location_list


def get_all_labour_types(response):
    labour_types = []
    data = response['results']
    if data:
        if data[0]['timeAndMaterials']['projectBillingRates']:
            labour_types = list(map(lambda item: {
               'billingRate': {'uri': item['billingRate']['uri']}
            }, data[0]['timeAndMaterials']['projectBillingRates']))
    return labour_types
