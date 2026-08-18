import rail

def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def filter_servicecenters_data(response):
    if not response['rows']:
        return []
    return list(map(lambda service_center: {
        "name": get_value(service_center, 0, "textValue"),
        "code": get_value(service_center, 1, "textValue"),
        "description": get_value(service_center, 2, "textValue"),
        "uri": get_value(service_center, 3, "uri")
    }, response['rows']))

def filter_divisions_data(response):
    if not response['rows']:
        return []
    return list(map(lambda division: {
        "name": get_value(division, 0, 'textValue'),
        "uri":  get_value(division, 1, 'uri'),
    }, response['rows']))

def filter_group_data(res):
    return list(
        map(lambda item:
            {
                'name': get_value(item, 0, 'textValue'),
                'uri': get_value(item, 0, 'uri'),
                'code': get_value(item, 1, 'textValue'),
            }, res['rows'])
    )

def get_filtered_user_data(response,dag_run):
    return list(filter(lambda x: bool(x['employeeid']) and x['employeeid'] == dag_run.conf['employeeid'], map(lambda row: {
        "name": get_value(row, 0, 'textValue'),
        'loginname': get_value(row, 1, 'textValue'),
        "uri": get_value(row, 0, 'uri'),
        "employeeid": get_value(row, 2, 'textValue'),
        "status": get_value(row, 3, 'textValue')
    }, response['rows'])))

def map_response_data(res):
    return list(
        map(lambda item:
            {
                'name': item['displayText'],
                'uri': item['uri'],
            }, res)
    )

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'serviceCenter', 'costCenter',  'division', 'employeeType']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))
