from datetime import datetime,timedelta
from math import ceil
import ast
import json
import logging
from dateutil.relativedelta import relativedelta, SA, FR
from pandas import DateOffset, to_datetime
import rail

from crl.user_import_united_kingdom import config as uk_config

logger = logging.getLogger(__name__)

GROUPS_DELIMITER = '|'
DATE_FORMAT = "%m/%d/%Y"

# Replicon policy-set scripts whose entitlement amount must be prorated for part-time
# employees. Annual leave carries the entitlement in a yearly accrual script; Sick
# leave has no accrual rule and instead carries it in a "Starting Balance Set To"
# rule. Map: script name -> parameter keyUri to scale.
PRORATION_SCRIPT_TARGETS = {
    'Yearly/Monthly Accrual with Expiry & Rounding': 'urn:replicon:script-key:parameter:yearly-entitlement',
    'Starting Balance Set To': 'urn:replicon:script-key:parameter:amount',
}


def round_up_to_half(value):
    """Round UP to the nearest 0.5 (spec: 20.15 -> 20.5, 20.51 -> 21). A small epsilon
    absorbs floating-point error so a value mathematically on the 0.5 grid (e.g. the
    product 25 * 0.62 = 15.500000000000002) is not spuriously bumped up half a day."""
    return ceil(float(value) * 2 - 1e-9) / 2


def parse_office_schedule_working_days(response):
    """Derive the working-day count and weekly hours from a GetOfficeScheduleDetails
    response. Only a simple 7-day pattern is supported; anything else is flagged
    invalid so the caller can surface an exception rather than silently mis-prorate."""
    if not response or not response.get('simplePattern'):
        return {'is_valid': False, 'working_days': 0, 'hours_per_week': 0.0,
                'details': 'Office schedule not found or does not follow a simple 7 day pattern'}

    simple_pattern = response['simplePattern']
    working_days = 0
    hours_per_week = 0.0
    for day in range(1, 8):
        duration = simple_pattern.get(f'day{day}WorkDuration') or {}
        day_hours = (duration.get('hours') or 0) + (duration.get('minutes') or 0) / 60
        if day_hours > 0:
            working_days += 1
            hours_per_week += day_hours

    if working_days == 0 or hours_per_week == 0:
        return {'is_valid': False, 'working_days': 0, 'hours_per_week': 0.0,
                'details': 'Office schedule has 0 working days / hours per week'}

    return {'is_valid': True, 'working_days': working_days,
            'hours_per_week': hours_per_week, 'details': ''}


def get_pt_proration_ratio(dag_run):
    """Return the part-time proration ratio for Annual/Sick leave, or None when
    proration does not apply (full-time employee or out-of-scope company).

    - 3040 (hours based): std_hrs / full-time weekly hours.
    - 3000/3080/3300 (days based): part-time working days / full-time working days,
      where the part-time working days come from the assigned time-off schedule
      (result of the 'get_office_schedule_details' task).

    Raises ValueError for an in-scope part-time employee whose schedule/hours cannot
    be determined, so the failure is logged instead of assigning full-time entitlement.
    """
    company_code = dag_run.conf.get('company_code')
    if dag_run.conf.get('full_part') != 'Part-Time':
        return None

    if company_code in uk_config.FT_WEEKLY_HOURS:          # hours based (3040)
        std_hrs = dag_run.conf.get('std_hrs')
        if not std_hrs:
            raise ValueError(
                f"Cannot prorate time off: Standard Hours missing for part-time employee (company {company_code})")
        return float(std_hrs) / uk_config.FT_WEEKLY_HOURS[company_code]

    if company_code in uk_config.FT_WORKING_DAYS:          # days based (3000/3080/3300)
        schedule = rail.result('get_office_schedule_details')
        if not schedule or not schedule.get('is_valid'):
            raise ValueError(
                "Cannot prorate time off for part-time employee: "
                f"{schedule.get('details') if schedule else 'office schedule details unavailable'}")
        return schedule['working_days'] / uk_config.FT_WORKING_DAYS[company_code]

    return None


def prorate_policy_set(policy_set, ratio):
    """Scale the entitlement amount inside a policy set by the part-time ratio and
    round up to the nearest 0.5. Handles both leave shapes: Annual leave's yearly
    accrual and Sick leave's "Starting Balance Set To". Reset/validation scripts are
    left untouched."""
    for script in policy_set.get('timeOffBalanceEventScripts', []):
        script_definition = script.get('script') or script.get('scriptTarget') or {}
        target_key = PRORATION_SCRIPT_TARGETS.get(script_definition.get('name'))
        if not target_key:
            continue
        for parameter in script.get('additionalParameters', []):
            if parameter.get('keyUri') == target_key:
                number = parameter.get('value', {}).get('number')
                if number not in (None, ''):
                    original = float(number)
                    new_value = round_up_to_half(original * ratio)
                    parameter['value']['number'] = new_value
                    logger.info("[PT proration] %s %s -> %s (ratio %.4f)",
                                script_definition.get('name'), original, new_value, ratio)
    return policy_set


def apply_pt_proration(policy_list, dag_run, loop_task_id):
    """Prorate every tier's policy set for the time-off type currently being processed
    in the for-each loop, when the employee is a part-time in-scope worker."""
    if not policy_list:
        return policy_list

    loop_item = rail.result(loop_task_id) or {}
    if loop_item.get('timeoff_type_name') not in uk_config.PRORATION_TIME_OFF_TYPES:
        return policy_list

    ratio = get_pt_proration_ratio(dag_run)
    if not ratio or ratio == 1.0:
        return policy_list

    logger.info("[PT proration] prorating %s at ratio %.4f (company=%s, full_part=%s, std_hrs=%s)",
                loop_item.get('timeoff_type_name'), ratio, dag_run.conf.get('company_code'),
                dag_run.conf.get('full_part'), dag_run.conf.get('std_hrs'))

    for entry in policy_list:
        if entry.get('policySet'):
            prorate_policy_set(entry['policySet'], ratio)
    return policy_list

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
    punch_policy =  list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:time-punch", response))
    overtime_policy = list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:work-authorization", response))
    schedule_policy = list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:shift-schedule", response))
    return {
        "punch_policy": punch_policy,
        "overtime_policy": overtime_policy,
        "schedule_policy": schedule_policy
    }

def get_policy_to_assign(response, dag_run, loop_task_id='for_each_time_off_assign_default_policy'):
    if not response:
        return None
    res = list(map(lambda item: {
        'description': 'Added by integration'+ str(item['effectiveDate']),
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, response))
    res = apply_pt_proration(res, dag_run, loop_task_id)
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

def get_policy_to_assign_for_timeoff(response, dag_run):
    """
    Processes the default time-off policy schedule and returns policies to assign.

    Uses date-based comparison to correctly handle mixed offset units (days, months, years).
    - Qualifying policies: offset effective date <= change_effective_date
    - Future policies: offset effective date > change_effective_date

    The current policy is the one with the LATEST effective date among qualifying policies.
    """
    if not response:
        return None

    # Determine the reference dates based on rehire status
    if dag_run.conf['rehire'] == 'Yes':
        effective_date_str = dag_run.conf['start_date']
    else:
        effective_date_str = dag_run.conf['change_effective_date']

    effective_date_for_updates = datetime.strptime(effective_date_str, DATE_FORMAT)
    adjusted_hire_date = datetime.strptime(dag_run.conf['adjusted_hire_date'], DATE_FORMAT)

    qualifying_policies = []  # Policies where calculated effective date <= change_effective_date
    future_policies = []      # Policies where calculated effective date > change_effective_date

    for item in response:
        # Calculate the ACTUAL effective date using calendar math (handles days/months/years correctly)
        policy_effective_date = to_datetime(adjusted_hire_date) + get_offset(item)
        policy_effective_date = policy_effective_date.to_pydatetime()

        if policy_effective_date <= effective_date_for_updates:
            # This policy's tier has already been reached based on tenure
            qualifying_policies.append({
                'policy_effective_date': policy_effective_date,
                'policySet': item['policySet'],
                'item': item
            })
        else:
            # This policy's tier is in the future
            future_policies.append({
                'policy_effective_date': policy_effective_date,
                'policySet': item['policySet'],
                'item': item
            })

    annual_policy_to_assign = []

    # Select current policy: the one with the LATEST effective date among qualifying policies
    if qualifying_policies:
        current_policy = max(qualifying_policies, key=lambda x: x['policy_effective_date'])
        annual_policy_to_assign.append({
            "description": "effective on " + effective_date_str,
            "effectiveDate": {
                'year': effective_date_for_updates.year,
                'month': effective_date_for_updates.month,
                'day': effective_date_for_updates.day
            },
            "policySet": current_policy['policySet']
        })

    # Add future policies with their calculated effective dates (sorted by date)
    future_policies_sorted = sorted(future_policies, key=lambda x: x['policy_effective_date'])
    for policy in future_policies_sorted:
        annual_policy_to_assign.append({
            "description": "effective on " + policy['policy_effective_date'].strftime(DATE_FORMAT),
            "effectiveDate": {
                'year': policy['policy_effective_date'].year,
                'month': policy['policy_effective_date'].month,
                'day': policy['policy_effective_date'].day
            },
            "policySet": policy['policySet']
        })

    if not annual_policy_to_assign:
        return None
    return apply_pt_proration(annual_policy_to_assign, dag_run, 'for_each_time_off_type_policy')

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
