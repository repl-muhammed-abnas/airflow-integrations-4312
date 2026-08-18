from datetime import datetime,timedelta
from math import ceil
import ast
import json
from dateutil.relativedelta import relativedelta, SA, FR
from pandas import DateOffset, to_datetime
import rail

GROUPS_DELIMITER = '|'
DATE_FORMAT = "%m/%d/%Y"

def get_replicon_date(date_str):
    if not date_str:
        return None

    date = datetime.strptime(date_str, DATE_FORMAT)
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }

def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def filter_group_data(response):
    if not response['rows']:
        return []
    return list(map(lambda item: {
        "name": get_value(item, 0, "textValue"),
        "code": get_value(item, 1, "textValue"),
        "uri": get_value(item, 2, "uri")
    }, response['rows']))

def filter_employee_grp_data(response):
    return list(
        map(lambda data:
            {
                'name': get_value(data, 0, 'textValue'),
                'uri': get_value(data, 0, 'uri')
            }, response['rows'])
    )

def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return GROUPS_DELIMITER.join([item['textValue'] for item in full_path_list])

def filter_full_path_data(response):
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "name": get_value(data, 0, 'textValue'),
        "uri": get_value(data, 1, 'cellCollection')[-1]['uri'],
        "full_path": get_full_path(data['cells'][1]['cellCollection'])
    }, response['rows']))

def get_policy_to_assign(response,dag_run,for_each_loop,config):
    if not response:
        return None
    res = list(map(lambda item: {
        'description': 'Added by integration'+ str(item['effectiveDate']),
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

def get_filtered_time_off_types(response):
    return list(map(lambda item: {
        "timeoff_type_name": item['displayText'],
        'timeoff_type_uri': item['uri'],
    }, response))

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})

def get_group_location_level_2_value(data):
    if not data:
        return {}
    return data[0].get("location",{}).get("parent",{}).get("location",{})

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'employeeType', 'costCenter', 'division', 'serviceCenter','department']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

    rail.set_result(key="location_level_2", val=get_group_location_level_2_value(response.get("locations")))

def map_assigned_policy_to_user(response):
    punch_policy =  list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:time-punch", response))
    overtime_policy = list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:work-authorization", response))
    schedule_policy = list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:shift-schedule", response))
    return {
        "punch_policy": punch_policy,
        "overtime_policy": overtime_policy,
        "schedule_policy": schedule_policy
    }

def get_missing_permissions(response, dag_run):
    permissions_to_add= []

    if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision'):
        permissions_to_add.append(dag_run.conf['supervisor_permission_uri'])

    if rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:user','permissionSet.displayText')!= "Report User with Substitute":
        permissions_to_add.append(dag_run.conf['report_user_substitute_permission_uri'])

    return permissions_to_add

def assigned_timeoffs_types_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        'timeoff_type_name': item['timeOffType']['displayText'],
        "timeoff_type_uri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    }, response['policiesByTimeOffType']))

def get_offset(item):
    if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'years':
        return DateOffset(years=int(item['startOffset']['offsetValue']))
    if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'months':
        return DateOffset(months=int(item['startOffset']['offsetValue']))
    return DateOffset(days=int(item['startOffset']['offsetValue']))

def get_policy_to_assign_for_timeoff(response,dag_run,for_each_loop, config):
    if not response:
        return None

    def get_rehire_effective_date():
        return dag_run.conf['start_date']

    def get_effective_date(item):
        present = datetime.strptime(dag_run.conf['change_effective_date']
            if dag_run.conf['rehire']!='Yes' else get_rehire_effective_date(),DATE_FORMAT).strftime(DATE_FORMAT)
        current_date = str(
            (to_datetime(present) + get_offset(item)).strftime(DATE_FORMAT))
        return get_replicon_date(current_date)
    
    if response and len(response)>1:
        policy_sets=[]
        policies_from_global = []
        policy_to_assign = []
        current_policy_to_assign = False

        effective_date_to_consider_for_update = dag_run.conf['change_effective_date']

        effective_date_for_updates = datetime.strptime(effective_date_to_consider_for_update, DATE_FORMAT)
        adjusted_hire_date = datetime.strptime(dag_run.conf['adjusted_hire_date'], DATE_FORMAT)
        tenure = float(ceil((effective_date_for_updates - adjusted_hire_date).days/365))

        if dag_run.conf['rehire']=='Yes':
            effective_date_for_rehire = get_replicon_date(dag_run.conf['start_date'])
            for policy in response:
                if policy['startOffset']['offsetValue']== 0:
                    policy_to_assign.append({
                        "description":"effective based on "+ dag_run.conf['start_date'],
                        "effectiveDate": effective_date_for_rehire,
                        "policySet": policy['policySet']
                    })
                else:
                    _date = to_datetime(dag_run.conf['start_date'], format=DATE_FORMAT) + get_offset(policy)
                    policy_to_assign.append({
                            "description":"effective on "+ dag_run.conf['start_date'],
                            "effectiveDate": {
                                    'year': _date.year,
                                    'month': _date.month,
                                    'day': _date.day
                                },
                            "policySet": policy['policySet']
                        })
            return policy_to_assign

        for items in response:
            if float(items['startOffset']['offsetValue']) > float(tenure):
                policy_sets.append({
                    "offsetValue": items['startOffset']['offsetValue'],
                    "policySet": items['policySet'],
                    "first":"No",
                    "policy": items
                })

            if float(items['startOffset']['offsetValue']) == float(tenure):
                policy_sets.append({
                    "offsetValue": items['startOffset']['offsetValue'],
                    "policySet": items['policySet'],
                    "first":"Yes",
                    "policy": items
                })

                current_policy_to_assign = True

        if not current_policy_to_assign:
            for items in response:
               if float(items['startOffset']['offsetValue']) < float(tenure):
                    policies_from_global.append({
                        "offsetValue": items['startOffset']['offsetValue'],
                        "policySet": items['policySet'],
                        "diff": float(items['startOffset']['offsetValue']) - float(tenure),
                        "policy": items
                    })

            if policies_from_global:
                max_diff = max(policies_from_global, key=lambda x: x["diff"])
                for policies in policies_from_global:
                    if float(policies["diff"]) == float(max_diff['diff']):
                        policy_sets.append({
                            "offsetValue": policies['offsetValue'],
                            "policySet": policies['policySet'],
                            "first": "Yes",
                            "policy": policies['policy']
                        })

        for items in policy_sets:
            if items['first']=="Yes":
                policy_to_assign.append({
                    "description":"effective on "+ str(effective_date_to_consider_for_update),
                    "effectiveDate": {
                            'year': effective_date_for_updates.year,
                            'month': effective_date_for_updates.month,
                            'day': effective_date_for_updates.day
                        },
                    "policySet": items['policySet']
                })
            else:
                _date = to_datetime(dag_run.conf['adjusted_hire_date'], format=DATE_FORMAT) + get_offset(items['policy'])
                policy_to_assign.append({
                    "description":"effective on "+ str(effective_date_to_consider_for_update),
                    "effectiveDate": {
                            'year': _date.year,
                            'month': _date.month,
                            'day': _date.day
                        },
                    "policySet": items['policySet']
                })
        return policy_to_assign

    return list(filter(lambda x: bool(x['effectiveDate']), map(lambda item: {
        'description': 'effective on '+ dag_run.conf['change_effective_date'] if dag_run.conf['rehire']!='Yes' else dag_run.conf['start_date'],
        'effectiveDate': get_effective_date(item),
        'policySet': item['policySet']
    }, response)))

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))

def assigned_time_offs_types_to_user(response,dag_run, mannual_time_off_types):
    if not response:
        return None
    return list(filter(lambda x: x['timeoff_type_name'] not in mannual_time_off_types
            if dag_run.conf['action']=='update' else x['timeoff_type_name'],map(lambda item: {
        'timeoff_type_name': item['timeOffType']['displayText'],
        "timeoff_type_uri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    }, response['policiesByTimeOffType'])))

def map_assigned_place_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        "effective_date": item['effectiveDate'],
        "place_name": item['places'][0]['displayText'] if item['places'] else None,
        'place_uri': item['places'][0]['uri'] if item['places'] else  None,
    }, response))

def get_filtered_place_details(response):
    return list(map(lambda item: {
        "place_name": item['name'],
        'place_uri': item['uri'],
    }, response))

def get_hidden_oef_value(response, hidden_oefs_names):
    return list(filter(lambda x: x['hidden_oef_name'] in hidden_oefs_names , map(lambda row: {
        "hidden_oef_name": row['cells'][0]['textValue'],
        "hidden_oef_uri": row['cells'][1]['uri'],
    }, response['rows'])))
