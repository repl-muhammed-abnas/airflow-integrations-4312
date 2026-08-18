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


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

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

def get_missing_permissions(response, dag_run):
    permissions_to_add= []

    if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision'):
        permissions_to_add.append(dag_run.conf['supervisor_permission_uri'])

    if rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:user','permissionSet.displayText')!= "Report User with Substitute":
        permissions_to_add.append(dag_run.conf['report_user_substitute_permission_uri'])

    return permissions_to_add


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


def filter_employee_grp_data(response):
    return list(
        map(lambda data:
            {
                'name': get_value(data, 0, 'textValue'),
                'uri': get_value(data, 0, 'uri')
            }, response['rows'])
    )


def get_filtered_time_off_types(response):
    return list(map(lambda item: {
        "timeoff_type_name": item['displayText'],
        'timeoff_type_uri': item['uri'],
    }, response))

def get_filtered_place_details(response):
    return list(map(lambda item: {
        "place_name": item['name'],
        'place_uri': item['uri'],
    }, response))

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
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
    return list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:time-punch", response))


def get_first_saturday_of_next_month(input_date_string):
    input_date = datetime.strptime(input_date_string,DATE_FORMAT)
    next_month = input_date.replace(day=1) +relativedelta(months=1)
    first_saturday = next_month+ relativedelta(weekday=SA(1))
    return {
        'year': first_saturday.year,
        'month': first_saturday.month,
        'day': first_saturday.day
    }

def get_first_friday_of_next_month(input_date_string):
    input_date = datetime.strptime(input_date_string,DATE_FORMAT)
    next_month = input_date.replace(day=1) +relativedelta(months=1)
    first_friday = next_month+ relativedelta(weekday=FR)
    return {
        'year': first_friday.year,
        'month': first_friday.month,
        'day': first_friday.day
    }

def get_upcoming_friday(input_date_string):
    input_date = to_datetime(input_date_string,format=DATE_FORMAT)
    days_to_add_for_friday = (4-input_date.day_of_week)%7
    upcoming_friday = input_date+DateOffset(days=days_to_add_for_friday)
    if input_date.day_of_week not in [6,0,1,2]:
        upcoming_friday += DateOffset(weeks=1)
    return (upcoming_friday).strftime(DATE_FORMAT)


def get_modified_policy(policy_set, dag_run, config, timeoff_type_name):

    if timeoff_type_name == "[USA] Floating Holiday":
        placeholder_timeoff_type = rail.find_first_by_attr_and_get_attr(config.FLOATING_HOLIDAY_TO_PLACEHOLDER,
                    "holiday_calendar", dag_run.conf['holiday_calendar'], "placeholder_timeoff_type")

        if not placeholder_timeoff_type:
            return []

        if placeholder_timeoff_type=="[USA] Floating Holiday NA05":
            return policy_set

        def get_balance_amount():
            # pylint: disable=too-many-return-statements
            # pylint: disable=too-many-branches
            month = datetime.strptime(dag_run.conf['start_date'],DATE_FORMAT).month

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_1"):
                if month ==12:
                    return 16
                return 16 if month <=6 else 8

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_3"):
                if month ==12:
                    return 24
                return 24 if month <=6 else (16 if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_4"):
                if month ==12:
                    return 28
                return 28 if month <=6 else ((16 if dag_run.conf['holiday_calendar'] == "MKPHC" else 20) if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_5"):
                if month ==12:
                    return 32
                return 32 if month <=6 else ((16 if dag_run.conf['holiday_calendar'] == "DURFRDSKOHC" else 20) if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_8"):
                if month ==12:
                    return 44
                return 44 if month <=3 else (32 if month <=6 else ((20 if dag_run.conf['holiday_calendar'] == "RALHC" else 24)
                    if month <=9 else (8 if dag_run.conf['holiday_calendar'] == "RALHC" else 16)))

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_2"):
                if month ==12:
                    return 20
                return 20 if month <=6 else (12 if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_6"):
                if month ==12:
                    return 36
                return 36 if month <=3 else (32 if month <=6 else (24 if month <=9 else (16 if month <=10 else 8)))

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_7"):
                if month ==12:
                    return 40
                return 40 if month <=3 else (24 if month <=6 else (16 if month <=9 else 8))

            return 0


        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": get_balance_amount()}
                        break
            if dag_run.conf['location_level_2_to_consider_for_timeoff'] in\
                config.CARRY_OVER_EXCEPTION_LOCATIONS_FLOATING_HOLIDAY:
                if item['script']['name'] == 'Yearly Reset':
                    for value in item['additionalParameters']:
                        if value['keyUri']=="urn:replicon:script-key:parameter:reset-balance-amount":
                            value['value']={"number": get_yearly_balance_for_placeholder(placeholder_timeoff_type, config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES)}
                            break

    if timeoff_type_name == "[USA] Emergency Leave":
        def get_amount():
            if dag_run.conf['location_level_2_to_consider_for_timeoff'] in ["California", "New York", "Colorado"]:
                return float(dag_run.conf['std_hrs'])*2.0
            return float(dag_run.conf['std_hrs'])

        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": get_amount()}
                        break
            if item['script']['name'] == 'Yearly Accrual':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:accrual-annual-amount":
                        value['value']={"number": get_amount()}
                        break

    return policy_set

def get_policy_to_assign(response,dag_run,for_each_loop,config):
    if not response:
        return None

    def get_effective_date(item):
        if rail.result(for_each_loop)['timeoff_type_name'] =="[USA] Volunteer Day":
            return get_first_saturday_of_next_month(dag_run.conf['start_date'])

        if rail.result(for_each_loop)['timeoff_type_name'] =="[USA] Sick":
            return get_replicon_date(get_upcoming_friday(dag_run.conf['start_date']))

        if rail.result(for_each_loop)['timeoff_type_name'] =="[USA] Floating Holiday":
            date_obj = datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)
            if date_obj.month!=12:
                return get_replicon_date(dag_run.conf['start_date'])

            return {
                'year': date_obj.year+1,
                'month': 1,
                'day': 1
            }

        return item['effectiveDate']


    res = list(map(lambda item: {
        'description': 'Added by integration'+ str(item['effectiveDate']),
        'effectiveDate': get_effective_date(item),
        'policySet': get_modified_policy(item['policySet'],dag_run, config, rail.result(for_each_loop)['timeoff_type_name'])
            if rail.result(for_each_loop)['timeoff_type_name'] in ["[USA] Floating Holiday", "[USA] Emergency Leave"]  else item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

def get_offset(item):
    if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'years':
        return DateOffset(years=int(item['startOffset']['offsetValue']))
    if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'months':
        return DateOffset(months=int(item['startOffset']['offsetValue']))
    return DateOffset(days=int(item['startOffset']['offsetValue']))

def get_policy_to_assign_for_vacation_add(response,dag_run):
    if not response:
        return None

    def get_effective_date(item):
        if item['startOffset']['offsetValue']== 0:
            return get_first_friday_of_next_month(dag_run.conf['adjusted_hire_date'])

        _date = to_datetime(dag_run.conf['adjusted_hire_date'], format=DATE_FORMAT) + get_offset(item)
        return {
                'year': _date.year,
                'month': 1,
                'day': 1
            }

    res= list(map(lambda item: {
        'description': 'effective on '+ str(get_first_friday_of_next_month(dag_run.conf['adjusted_hire_date'])),
        'effectiveDate': get_effective_date(item),
        'policySet': item['policySet']
    }, response))

    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

def assigned_timeoffs_types_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        'timeoff_type_name': item['timeOffType']['displayText'],
        "timeoff_type_uri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    }, response['policiesByTimeOffType']))

def validate_adjusted_hire_date_updated(dag_run):
    return bool(dag_run.conf['previous_adjusted_hire_date'] and get_date_from_replicon_date(dag_run.conf['previous_adjusted_hire_date']
        )!= get_date_from_replicon_date(get_replicon_date(dag_run.conf['adjusted_hire_date'])))

def get_policy_to_assign_for_timeoff(response,dag_run,for_each_loop, config):
    if not response:
        return None

    def get_rehire_effective_date():
        if rail.result(for_each_loop)['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TO_TYPES and\
            (dag_run.conf['previous_employee_status'] =="Unpaid Leave" or dag_run.conf['previous_employee_status'] =="Paid Leave"):
            if dag_run.conf['assigned_event_reason_code']=='10' and \
                list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
                return dag_run.conf['change_effective_date']
        return dag_run.conf['start_date']

    def get_effective_date(item):
        present = datetime.strptime(dag_run.conf['change_effective_date']
            if dag_run.conf['rehire']!='Yes' else get_rehire_effective_date(),DATE_FORMAT).strftime(DATE_FORMAT)
        current_date = str(
            (to_datetime(present) + get_offset(item)).strftime(DATE_FORMAT))
        return get_replicon_date(current_date)

    if rail.result(for_each_loop)['timeoff_type_name'] == "[USA] Vacation":
        policy_sets=[]
        policies_from_global = []
        vacation_policy_to_assign = []
        current_policy_to_assign = False

        is_adjusted_hire_date_updated = validate_adjusted_hire_date_updated(dag_run)

        effective_date_to_consider_for_update = dag_run.conf['todays_date'] if is_adjusted_hire_date_updated else dag_run.conf['change_effective_date']

        effective_date_for_updates = datetime.strptime(effective_date_to_consider_for_update, DATE_FORMAT)
        adjusted_hire_date = datetime.strptime(dag_run.conf['adjusted_hire_date'], DATE_FORMAT)
        tenure = float(ceil((effective_date_for_updates - adjusted_hire_date).days/365))

        if dag_run.conf['rehire']=='Yes':
            effective_date_for_rehire = get_first_friday_of_next_month(dag_run.conf['start_date'])
            for policy in response:
                if policy['startOffset']['offsetValue']== 0:
                    vacation_policy_to_assign.append({
                        "description":"effective based on "+ dag_run.conf['start_date'],
                        "effectiveDate": effective_date_for_rehire,
                        "policySet": policy['policySet']
                    })
                else:
                    _date = to_datetime(dag_run.conf['start_date'], format=DATE_FORMAT) + get_offset(policy)
                    vacation_policy_to_assign.append({
                            "description":"effective on "+ dag_run.conf['start_date'],
                            "effectiveDate": {
                                    'year': _date.year,
                                    'month': 1,
                                    'day': 1
                                },
                            "policySet": policy['policySet']
                        })
            return vacation_policy_to_assign

        if rail.find_first_by_attr_and_get_attr(dag_run.conf['time_off_types_to_assign'],
                "actual_timeoff_type_name","[USA] Vacation","placeholder_timeoff_type_name") == "[USA] Vacation":

            def get_vp_policy_set():
                current_job = dag_run.conf.get('current_assigned_job_code') or ''
                new_job = dag_run.conf.get('job_code') or ''
                is_promotion_to_vp = (
                    (len(new_job) >= 2 and new_job[-2:] in config.VP_JOB_CODES_SUFFIX) and
                    (current_job and current_job[-2:] not in config.VP_JOB_CODES_SUFFIX)
                )
                starting_balance = 0 if is_promotion_to_vp else rail.result('get_balance_summary_for_user_vacation_to_type')['timeRemaining']
                return {
                        "timeOffBalanceEventScripts":[{
                        "script": {
                            "description": "Set initial balance for the first day of a policy",
                            "name": "Starting Balance Set To",
                            "uri": dag_run.conf['starting_balance_script_uri']
                        },
                        "additionalParameters": [{
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                            "number": starting_balance
                            }
                        }]
                        }],
                        "timeOffValidationScripts": []
                    }

            return [{
                    "description":"effective on "+ str(effective_date_to_consider_for_update),
                    "effectiveDate": {
                            'year': effective_date_for_updates.year,
                            'month': effective_date_for_updates.month,
                            'day': effective_date_for_updates.day
                        },
                    "policySet": get_vp_policy_set()
                }]

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
                vacation_policy_to_assign.append({
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
                vacation_policy_to_assign.append({
                    "description":"effective on "+ str(effective_date_to_consider_for_update),
                    "effectiveDate": {
                            'year': _date.year,
                            'month': 1,
                            'day': 1
                        },
                    "policySet": items['policySet']
                })
        return vacation_policy_to_assign


    return list(filter(lambda x: bool(x['effectiveDate']), map(lambda item: {
        'description': 'effective on '+ dag_run.conf['change_effective_date'] if dag_run.conf['rehire']!='Yes' else dag_run.conf['start_date'],
        'effectiveDate': get_effective_date(item),
        'policySet': item['policySet'] if rail.result(for_each_loop)['timeoff_type_name'] not in ["[USA] Floating Holiday", "[USA] Emergency Leave"]
            else get_modified_policy_update(dag_run,item['policySet'], rail.result(for_each_loop)['timeoff_type_name'], config),
    }, response)))

def map_assigned_place_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        "effective_date": item['effectiveDate'],
        "place_name": item['places'][0]['displayText'] if item['places'] else None,
        'place_uri': item['places'][0]['uri'] if item['places'] else  None,
    }, response))

def get_hidden_oef_value(response, hidden_oefs_names):
    return list(filter(lambda x: x['hidden_oef_name'] in hidden_oefs_names , map(lambda row: {
        "hidden_oef_name": row['cells'][0]['textValue'],
        "hidden_oef_uri": row['cells'][1]['uri'],
    }, response['rows'])))

def get_modified_termination_sick_policy(policy_set):
    for item in policy_set['timeOffBalanceEventScripts']:
        if item['script']['name'] == 'Starting Balance Set To':
            for value in item['additionalParameters']:
                if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                    value['value']={"number": rail.result('get_balance_summary_for_user')['timeRemaining']}
                    break
        if item['script']['name'] == 'Yearly Carry Over with Expiry':
            for value in item['additionalParameters']:
                if value['keyUri']=="urn:replicon:script-key:parameter:carry-up-to-amount":
                    value['value']={"number": rail.result('get_balance_summary_for_user')['timeRemaining']}
                    break

    return policy_set

def get_termination_policyset_sick_timeoff_type(response,dag_run,action):
    effective_date = (datetime.strptime(dag_run.conf['end_date'],DATE_FORMAT)+timedelta(days=1)).strftime(DATE_FORMAT)\
        if action =='disable' and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']

    return list(filter(lambda x: bool(x['effectiveDate']), map(lambda item: {
        'description': 'effective on '+ str(effective_date),
        'effectiveDate': get_replicon_date(effective_date),
        'policySet': get_modified_termination_sick_policy(item['policySet']),
    }, response)))


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

def get_yearly_balance_for_placeholder(placeholder_timeoff_type, floating_holiday_names):
    # Returns the Yearly value for a given placeholder
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_1"):
        return 16
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_2"):
        return 20
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_3"):
        return 24
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_4"):
        return 28
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_5"):
        return 32
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_6"):
        return 36
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_7"):
        return 40
    if placeholder_timeoff_type == floating_holiday_names.get("placeholder_8"):
        return 44
    return 0

def get_modified_policy_update(dag_run, policy_set, timeoff_type_name, config):

    if timeoff_type_name == "[USA] Floating Holiday":

        placeholder_timeoff_type = rail.find_first_by_attr_and_get_attr(config.FLOATING_HOLIDAY_TO_PLACEHOLDER,
                    "holiday_calendar", dag_run.conf['holiday_calendar'], "placeholder_timeoff_type")

        if not placeholder_timeoff_type:
            return []

        if placeholder_timeoff_type=="[USA] Floating Holiday NA05":
            return policy_set

        def get_balance_amount():
            # pylint: disable=too-many-return-statements
            # pylint: disable=too-many-branches
            get_effective_date_to_consider = dag_run.conf['start_date'] if dag_run.conf['rehire']=="Yes" else dag_run.conf['change_effective_date']
            month = datetime.strptime(get_effective_date_to_consider,DATE_FORMAT).month

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_1"):
                if month ==12:
                    return 16
                return 16 if month <=6 else 8

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_3"):
                if month ==12:
                    return 24
                return 24 if month <=6 else (16 if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_4"):
                if month ==12:
                    return 28
                return 28 if month <=6 else ((16 if dag_run.conf['holiday_calendar'] == "MKPHC" else 20) if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_5"):
                if month ==12:
                    return 32
                return 32 if month <=6 else ((16 if dag_run.conf['holiday_calendar'] == "DURFRDSKOHC" else 20) if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_8"):
                if month ==12:
                    return 44
                return 44 if month <=3 else (32 if month <=6 else ((20 if dag_run.conf['holiday_calendar'] == "RALHC" else 24)
                    if month <=9 else (8 if dag_run.conf['holiday_calendar'] == "RALHC" else 16)))

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_2"):
                if month ==12:
                    return 20
                return 20 if month <=6 else (12 if month <=9 else 8)

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_6"):
                if month ==12:
                    return 36
                return 36 if month <=3 else (32 if month <=6 else (24 if month <=9 else (16 if month <=10 else 8)))

            if placeholder_timeoff_type == config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES.get("placeholder_7"):
                if month ==12:
                    return 40
                return 40 if month <=3 else (24 if month <=6 else (16 if month <=9 else 8))

            return 0


        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": get_balance_amount()}
                        break

            if item['script']['name'] == 'Yearly Reset':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:reset-balance-amount":
                        value['value']={"number": 0 if dag_run.conf['location_level_2_to_consider_for_timeoff'] not in
                            config.CARRY_OVER_EXCEPTION_LOCATIONS_FLOATING_HOLIDAY else
                            get_yearly_balance_for_placeholder(placeholder_timeoff_type, config.FLOATING_HOLIDAY_PLACEHOLDER_NAMES)}
                        break

    if timeoff_type_name == "[USA] Emergency Leave":
        def get_amount():
            if dag_run.conf['location_level_2_to_consider_for_timeoff'] in ["California", "New York", "Colorado"]:
                return float(dag_run.conf['std_hrs'])*2.0
            return float(dag_run.conf['std_hrs'])

        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": get_amount()}
                        break
            if item['script']['name'] == 'Yearly Accrual':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:accrual-annual-amount":
                        value['value']={"number": get_amount()}
                        break
    return policy_set
