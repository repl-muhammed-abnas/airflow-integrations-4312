from datetime import datetime
import rail


def get_filtered_user_data(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: bool(x['employeeid']) and x['employeeid'] == dag_run.conf['employeeid'], map(lambda row: {
        "name": row['cells'][0]['textValue'],
        'loginname': row['cells'][1]['textValue'],
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][2]['textValue'] if row['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][3]['textValue']
    }, data['rows'])))


def get_filtered_employee_grp(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri'],
        "code": row['cells'][1]['textValue'],
    }, data['rows']))


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def map_supervisor_list_data(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: x['employeeid'] == dag_run.conf['reportstoid'], map(lambda item: {
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
                                  ['name'] == 'Project Resource with Reports']) > 0

    if not supervisor_permission:
        permissions_to_add.append(dag_run.conf['timesheetapproveruri'])

    if not end_user_permission:
        permissions_to_add.append(dag_run.conf['enduseruri'])

    return permissions_to_add


def get_filtered_department(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "name": row['displayText'],
        'uri': row['uri'],
    }, data['department']['childDepartments']))


def get_user_time_off_assigned(response):
    data = response.json()['d']['policiesByTimeOffType']
    modified_data = list(map(lambda x: {
        "name": x["timeOffType"]["name"],
        "uri": x["timeOffType"]["uri"],
        "effectiveDate": x['policySetSchedule'][0]['effectiveDate'] if x['policySetSchedule'] else None,
        "isTimeOffAllowedAgainstThisTimeOffType": x['isTimeOffAllowedAgainstThisTimeOffType']}, data))

    rail.set_result(key="disabled_timeoffs", val=list(filter(lambda timeoff: timeoff['isTimeOffAllowedAgainstThisTimeOffType'] is False, modified_data)))
    rail.set_result(key="all_users_tos", val=list(map(lambda item: item['name'],modified_data)))

    return list(map(lambda x: {
        "name": x["timeOffType"]["name"],
        "uri": x["timeOffType"]["uri"],
        "effectiveDate": x['policySetSchedule'][0]['effectiveDate'] if x['policySetSchedule'] else None},
        list(filter(lambda row: row['isTimeOffAllowedAgainstThisTimeOffType'] is True, data))))


def get_specific_user_time_off_assigned(response, dag_run):
    data = response.json()['d']['policiesByTimeOffType']
    return list(filter(lambda x: x['timeOffType']['displayText'] == dag_run.conf['timeofftypename'], data))[0]['policySetSchedule']


def get_user_time_off(response):
    data = response.json()['d']['policiesByTimeOffType']
    return list(filter(lambda x: x['isTimeOffAllowedAgainstThisTimeOffType'] is True, data))


def get_user_future_time_off(response):
    data = response.json()['d']['policiesByTimeOffType']
    time_off_present = list(
        filter(lambda x: x['isTimeOffAllowedAgainstThisTimeOffType'] is True, data))
    time_off_disable = rail.result("time_off_types_to_disable")
    return list(filter(lambda x: x['timeOffType']['displayText'] in time_off_disable, time_off_present))


def map_time_off_delete_uri(response):
    time_off_list = []
    data = response.json()['d']['rows']
    for time_off in data:
        time_off_list.append(time_off['cells'][0]['uri'])
    return time_off_list


def get_file_id_uri(response):
    data = response.json()['d']
    for custom_feild in data:
        if custom_feild["displayText"] == "File ID":
            return custom_feild["uri"]
    return ""


def map_assigned_policy_to_user(response):
    data = response.json()['d']
    return list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:time-punch", data))


def get_specfic_time_off_types(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == dag_run.conf['timeofftype'], data))

def get_weekly_rule_cn_uri(response):
    if not response['rows']:
        raise Exception("No Cost Normalization rules found in instance")
    cn_data = list(filter(lambda item: item['cells'][0]['textValue'] == "Weekly Rule", response['rows']))
    if not cn_data:
        raise Exception("`Weekly Rule Cost Normalization not found")
    return cn_data[0]['cells'][0]['uri']

def get_updated_cost_normalization_filter(response):
    cost_normalization_data = response['entries']
    if not cost_normalization_data:
        return None
    if not cost_normalization_data[-1]['endDate']:
        rail.set_result(key="last_cost_normalization_uri",val= cost_normalization_data[-1]['uri'])
    return cost_normalization_data
