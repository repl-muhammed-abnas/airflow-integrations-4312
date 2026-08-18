from datetime import datetime
import rail

from moodys.user_sync.japan.mapper.user_sync_mapper import user_sync_mapper


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def filter_group_data(res):
    return list(
        map(lambda item:
            {
                'name': get_value(item, 0, 'textValue'),
                'uri': get_value(item, 0, 'uri'),
                'code': get_value(item, 1, 'textValue'),
            }, res['rows'])
    )

def filter_divisions_data(response):
    if not response['rows']:
        return []
    return list(map(lambda division: {
        "name": get_value(division, 0, 'textValue'),
        "uri":  get_value(division, 1, 'uri'),
    }, response['rows']))

def map_response_data(res):
    return list(
        map(lambda item:
            {
                'name': item['displayText'],
                'uri': item['uri'],
            }, res)
    )

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))

def get_filtered_user_data(response,dag_run):
    return list(filter(lambda x: x['loginname'] == dag_run.conf['loginname'], map(lambda row: {
        "name": get_value(row, 0, 'textValue'),
        'loginname': get_value(row, 1, 'textValue'),
        "uri": get_value(row, 0, 'uri'),
        "employeeid": get_value(row, 2, 'textValue'),
        "status": get_value(row, 3, 'boolValue')
    }, response['rows'])))


def get_required_time_off_types(response):
    assignable_timeoffs_details = list(filter(lambda x: x['type'] == 'timeofftype' , user_sync_mapper))
    assignable_timeoffs_names = list(map(lambda item: item['timeofftypename'],assignable_timeoffs_details))
    return rail.write_json_artifact(list(filter(lambda x: x['timeofftypename'] in assignable_timeoffs_names, map(lambda item: {
        "timeofftypename": item['displayText'],
        'timeofftypeuri': item['uri'],
    }, response))))


def map_supervisor_list_data(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: x['loginname'] == dag_run.conf['supervisorid'], map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'employeeid': item['cells'][2]['textValue'] if item['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        'status': item['cells'][3]['textValue'],
        'enddate': get_date_from_replicon_date(item['cells'][4]['dateValue']).strftime("%m-%d-%Y") if item['cells'][4]['textValue'] != "" else None
    }, data['rows'])))

def get_missing_permissions(response, dag_run):
    supervisor_permission = False
    end_user_permission = False
    permissions_to_add = []
    if response:
        supervisor_permission = len(
            [x for x in response if x['permissionSet']['name'] == 'Supervisor']) > 0
        end_user_permission = len([x for x in response if x['permissionSet']
                                  ['name'] == 'End user with reports']) > 0

    if not supervisor_permission:
        permissions_to_add.append(dag_run.conf['supervisorpermissionuri'])

    if not end_user_permission:
        permissions_to_add.append(dag_run.conf['enduserwithreportspermissionuri'])

    return permissions_to_add

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department','division', 'employeeType']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))
