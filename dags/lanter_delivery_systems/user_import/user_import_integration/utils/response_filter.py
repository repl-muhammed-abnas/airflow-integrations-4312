from datetime import datetime
import rail

GROUPS_DELIMITER = '|'

null_urn = "urn:replicon:list-type:null"

def get_value(item, index, pluck_key):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else ""

def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return GROUPS_DELIMITER.join([item['textValue'] for item in full_path_list])

def groups_filter(response):
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "name": get_value(data['cells'], 0, 'textValue'),
        "uri": get_value(data['cells'], 1, 'cellCollection')[-1]['uri'],
        "full_path": get_full_path(data['cells'][1]['cellCollection'])
    }, response['rows']))

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))

def filter_group_data(res):
    return list(
        map(lambda data:
            {
                'name': get_value(data['cells'], 0, 'textValue'),
                'uri': get_value(data['cells'], 0, 'uri'),
                'code': get_value(data['cells'], 1, 'textValue'),
            }, res['rows'])
    )

def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def map_supervisor_list_data(response, dag_run):
    data = response['rows']
    return list(filter(lambda x: x['name'].lower() == dag_run.conf['supervisorusername'].lower(), map(lambda item: {
        'name': get_value(item['cells'], 0, 'textValue'),
        'loginname': get_value(item['cells'], 1, 'textValue'),
        'uri':  get_value(item['cells'], 0, 'uri'),
        'status':  get_value(item['cells'], 2, 'textValue')
    }, data)))

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'employeeType']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

def get_filtered_user_data(response):
    return [] if response == [None] else response

def filter_product_license_description(response):
    return list(map(lambda item:{
        'displayText': item['displayText'].lower(),
        'uri': item['uri']
    },response))
