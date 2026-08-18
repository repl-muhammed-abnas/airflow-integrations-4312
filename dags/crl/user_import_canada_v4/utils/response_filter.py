from datetime import datetime,timedelta
import ast
import json
from dateutil.relativedelta import relativedelta
from pandas import DateOffset, to_datetime
import rail

GROUPS_DELIMITER = '|'
DATE_FORMAT = "%m/%d/%Y"

def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

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

def get_missing_permissions(response, dag_run):
    permissions_to_add= []

    if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision'):
        permissions_to_add.append(dag_run.conf['supervisor_permission_uri'])

    if rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:user','permissionSet.displayText')!= "Report User with Substitute":
        permissions_to_add.append(dag_run.conf['report_user_substitute_permission_uri'])

    return permissions_to_add

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

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'employeeType', 'costCenter', 'division', 'serviceCenter']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

def map_assigned_policy_to_user(response):
    return list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:time-punch", response))

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

def get_filtered_time_off_types(response):
    return list(map(lambda item: {
        "timeoff_type_name": item['displayText'],
        'timeoff_type_uri': item['uri'],
    }, response))

# pylint: disable=too-many-branches
def get_modified_policy(policy_set, time_off_type_name, dag_run, user_value):
    if time_off_type_name == "[CAN] Jour personnel (temporaires)/Personal Days Temp" and dag_run.conf['reg_temp']=='Temporary':
        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": 16 if dag_run.conf['location_level_3'] =="STCONSTANT" else 15}
                        break
            if item['script']['name'] == 'Yearly Reset':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:reset-balance-amount":
                        value['value']={"number": 16 if dag_run.conf['location_level_3'] =="STCONSTANT" else 15}
                        break

    if time_off_type_name == "[CAN] Jour personnel/Personal Days" and dag_run.conf['reg_temp']=='Regular' \
        and dag_run.conf['location_level_3']!="STCONSTANT" and dag_run.conf['pay_type']=="Hourly":
        def get_balance_amount():
            if 1 <= datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT).month <=3:
                return 30
            if 3 < datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT).month <=6:
                return 22.50
            return 15

        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": get_balance_amount() if user_value =='add' else 30}
                        break

            if item['script']['name'] == 'Yearly Reset':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:reset-balance-amount":
                        value['value']={"number": 37.5 }
                        break



    if time_off_type_name == "[CAN] Jour personnel/Personal Days" and dag_run.conf['reg_temp']=='Regular' and dag_run.conf['location_level_3']=="STCONSTANT":
        def get_amount():
            if 1 <= datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT).month <=3:
                return 40
            if 3 < datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT).month <=6:
                return 32
            if 6 < datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT).month <=8:
                return 24
            if 8 < datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT).month <=9:
                return 16
            return 8

        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": get_amount() if user_value =='add' else 40 }
                        break

            if item['script']['name'] == 'Yearly Reset':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:reset-balance-amount":
                        value['value']={"number": 40 }
                        break

    if time_off_type_name =="[CAN] Journée Flexible/Flexible Day - St.Constant" \
        and dag_run.conf['reg_temp']=='Temporary' and dag_run.conf['location_level_3']=="STCONSTANT":
        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Starting Balance Set To':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                        value['value']={"number": 7.50 }
                        break
            if item['script']['name'] == 'Yearly Reset':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:reset-balance-amount":
                        value['value']={"number": 7.50 }
                        break

    return policy_set

def get_policy_to_assign(response,modify_default_policy,dag_run,for_each_loop):
    if not response:
        return None
    res = list(map(lambda item: {
        'description': 'effective on '+ str(item['effectiveDate']),
        'effectiveDate': item['effectiveDate'],
        'policySet': get_modified_policy(item['policySet'], rail.result(for_each_loop)['timeoff_type_name'],dag_run,'add')
            if rail.result(for_each_loop)['timeoff_type_name'] in modify_default_policy else item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

def get_policy_to_assign_for_timeoff(response,modify_default_policy,dag_run,for_each_loop, config):
    if not response:
        return None

    def get_offset(item):
        if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'years':
            return DateOffset(years=int(item['startOffset']['offsetValue']))
        if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'months':
            return DateOffset(months=int(item['startOffset']['offsetValue']))
        return DateOffset(days=int(item['startOffset']['offsetValue']))

    def get_rehire_effective_date():
        if rail.result(for_each_loop)['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES and\
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

    def get_effective_date_montreal_vacation(item):

        if dag_run.conf['rehire']!='Yes':
            change_effective_date = datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT)
            adjusted_hire_date = datetime.strptime(dag_run.conf['adjusted_hire_date'], DATE_FORMAT)
            tenure_months = (change_effective_date.year - adjusted_hire_date.year)*12+ change_effective_date.month-adjusted_hire_date.month

            if tenure_months > 12:
                if item['startOffset']['offsetValue']== 0:
                    return None
                return get_replicon_date(dag_run.conf['change_effective_date'])

            if item['startOffset']['offsetValue']== 0:
                return get_replicon_date(dag_run.conf['change_effective_date'])
            _date = (datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT)+relativedelta(months=13))
            return {
                'year': _date.year,
                'month': _date.month,
                'day': 1
            }

        if item['startOffset']['offsetValue']== 0:
            return get_replicon_date(dag_run.conf['start_date'])
        _date = (datetime.strptime(dag_run.conf['start_date'],DATE_FORMAT)+relativedelta(months=13))
        return {
                'year': _date.year,
                'month': _date.month,
                'day': 1
            }


    return list(filter(lambda x: bool(x['effectiveDate']), map(lambda item: {
        'description': 'effective on '+ dag_run.conf['change_effective_date'] if dag_run.conf['rehire']!='Yes' else dag_run.conf['start_date'],
        'effectiveDate': get_effective_date(item) if rail.result(for_each_loop)['timeoff_type_name'] != "[CAN] Vacances/Vacation"
                else get_effective_date_montreal_vacation(item),
        'policySet': get_modified_montreal_vacation_policy(item['policySet'], item['startOffset']['offsetValue'],
                dag_run.conf['std_hrs'],config.FULL_TIME_HRS_VACATION)
            if dag_run.conf['full_part'] =="Part-Time" and rail.result(for_each_loop)['timeoff_type_name'] == "[CAN] Vacances/Vacation" else
            (get_modified_policy(item['policySet'], rail.result(for_each_loop)['timeoff_type_name'],dag_run,'update')
            if rail.result(for_each_loop)['timeoff_type_name'] in modify_default_policy else item['policySet'])
    }, response)))


def assigned_timeoffs_types_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        'timeoff_type_name': item['timeOffType']['displayText'],
        "timeoff_type_uri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    }, response['policiesByTimeOffType']))


def get_filtered_place_details(response):
    return list(map(lambda item: {
        "place_name": item['name'],
        'place_uri': item['uri'],
    }, response))

def map_assigned_place_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        "effective_date": item['effectiveDate'],
        "place_name": item['places'][0]['displayText'] if item['places'] else None,
        'place_uri': item['places'][0]['uri'] if item['places'] else  None,
    }, response))

def get_updated_service_period(old_service_period, part_time_percentage):
    new_service_period =[]
    for item in old_service_period.split(","):

        splitted_value = item.split(":")
        splitted_value[-1] = str(round(float(splitted_value[-1])*part_time_percentage,2))
        item = ":".join(splitted_value)
        new_service_period.append(item)

    return ",".join(new_service_period)

def get_modified_montreal_vacation_policy(policy_set, offset_value, std_hrs, full_time_hrs):
    part_time_percentage = float(std_hrs)/full_time_hrs

    if offset_value==0:
        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Yearly/Monthly Accrual with Expiry & Rounding':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:yearly-entitlement":
                        value['value']={ "number": round(float(value['value']['number'])*part_time_percentage,2) }
                        break

            if item['script']['name'] == 'Yearly Carry Over with Expiry':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:carry-up-to-amount":
                        value['value']={ "number": round(float(value['value']['number'])*part_time_percentage,2) }
                        break

    if offset_value!=0:
        for item in policy_set['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'Accrual Based on Service Period':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:tenure-period":
                        value['value']={"text": get_updated_service_period(value['value']['text'], part_time_percentage)}
                        break

            if item['script']['name'] == 'Yearly Carry Over with Expiry':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:carry-up-to-amount":
                        value['value']={ "number": round(float(value['value']['number'])*part_time_percentage, 2) }
                        break

    for item in policy_set['timeOffValidationScripts']:
        if item['script']['name'] == 'Prevent overdraw at end of year':
            for value in item['additionalParameters']:
                if value['keyUri']=="urn:replicon:script-key:parameter:maximum-overdraw":
                    value['value']={ "number": round(float(value['value']['number'])*part_time_percentage, 2) }
                    break

    return policy_set

def get_policy_to_assign_for_montreal_vacation_add(response,dag_run, full_time_hrs):
    if not response:
        return None

    def get_effective_date(item):
        if item['startOffset']['offsetValue']== 0:
            return get_replicon_date(dag_run.conf['adjusted_hire_date'])
        _date = (datetime.strptime(dag_run.conf['adjusted_hire_date'],DATE_FORMAT)+relativedelta(months=13))
        return {
                'year': _date.year,
                'month': _date.month,
                'day': 1
            }

    res= list(map(lambda item: {
        'description': 'effective on '+ str(dag_run.conf['adjusted_hire_date']),
        'effectiveDate': get_effective_date(item),
        'policySet': get_modified_montreal_vacation_policy(item['policySet'],
            item['startOffset']['offsetValue'],dag_run.conf['std_hrs'], full_time_hrs) if dag_run.conf['full_part'] =="Part-Time" else item['policySet']
    }, response))

    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

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
