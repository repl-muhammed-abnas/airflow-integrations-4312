from datetime import datetime,timedelta
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


def get_policy_to_assign(response):
    if not response:
        return None

    res = list(map(lambda item: {
        'description': 'Added by integration'+ str(item['effectiveDate']),
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

def get_offset(item):
    if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'years':
        return DateOffset(years=int(item['startOffset']['offsetValue']))
    if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'months':
        return DateOffset(months=int(item['startOffset']['offsetValue']))
    return DateOffset(days=int(item['startOffset']['offsetValue']))

def assigned_timeoffs_types_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        'timeoff_type_name': item['timeOffType']['displayText'],
        "timeoff_type_uri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    }, response['policiesByTimeOffType']))

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

    return list(filter(lambda x: bool(x['effectiveDate']), map(lambda item: {
        'description': 'effective on '+ dag_run.conf['change_effective_date'] if dag_run.conf['rehire']!='Yes' else dag_run.conf['start_date'],
        'effectiveDate': get_effective_date(item),
        'policySet': item['policySet'],
    }, response)))

def map_assigned_place_to_user(response):
    if not response:
        return None
    return list(map(lambda item: {
        "effective_date": item['effectiveDate'],
        "place_name": item['places'][0]['displayText'] if item['places'] else None,
        'place_uri': item['places'][0]['uri'] if item['places'] else  None,
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
