from datetime import datetime
import json
from ast import literal_eval
from functools import lru_cache
from ipipeline.timeoff_policy_custom_script import config
import rail
null = None

# This is defined in config.py as well, if any change is required, please update there as well
DATE_FORMAT = "%Y/%m/%d"

def get_split_date(date_value, split_type='str'):
    if date_value and isinstance(date_value, str):
        date_value = datetime.strptime(date_value, config.DATE_DEFAULT_FORMAT)
    if split_type == 'int':
        return {
            'day': date_value.day,
            'month': date_value.month,
            'year': date_value.year
        }
    return {
        'day': date_value.strftime("%d"),
        'month': date_value.strftime("%m"),
        'year': date_value.strftime("%Y")
    }

def to_datetime(date, date_format=DATE_FORMAT):
    if isinstance(date, dict):
        return datetime(day=date['day'], month=date['month'], year=date['year'])
    elif isinstance(date, str):
        return datetime.strptime(date, date_format)
    return date


def get_relevant_historical_policies(existing_timeoff_policysetschedule, effective_date_derived):
    if bool(existing_timeoff_policysetschedule and existing_timeoff_policysetschedule[0] and existing_timeoff_policysetschedule[0]['description']):
        count = 0
        for item in existing_timeoff_policysetschedule:
            if to_datetime(item['effectiveDate']) < to_datetime(effective_date_derived):
                count += 1

        relevant_historical_policies = json.loads(json.dumps(existing_timeoff_policysetschedule[0:count]).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        return relevant_historical_policies

    return []

def get_default_policyline(timeoff_uri):
    return rail.find_first_by_attr_and_get_attr(rail.load_all_records(
        rail.result('get_default_policies')), 'timeOffUri',
        timeoff_uri,
        'defaultPolicy', []
    )

@lru_cache(maxsize=16)
def get_artifact_data(artifact_name):
    return rail.load_all_records(artifact_name)

def set_starting_balance_amount_in_policy_set(policy_set, yearly_accrual_rate, scheduled_hours):
    number = (yearly_accrual_rate) * (scheduled_hours/5)

    for x in policy_set['timeOffBalanceEventScripts']:
        if x['scriptTarget']['name'] == "Starting Balance Set To":
            for y in x['additionalParameters']:
                if y['keyUri'] == "urn:replicon:script-key:parameter:amount":
                    y['value']['number'] = round(float(number), 4)
    return null

def set_yearly_entitlement_amount_in_policy_set(policy_set, yearly_accrual_rate, scheduled_hours, fte):
    number = (yearly_accrual_rate) * (scheduled_hours/fte)
    for x in policy_set['timeOffBalanceEventScripts']:
        if x['scriptTarget']['name'] == "Monthly Accrual":
            for y in x['additionalParameters']:
                if y['keyUri'] == "urn:replicon:script-key:parameter:accrual-annual-amount":
                    y['value']['number'] = float(number)
    return null

def set_yearly_entitlement_amount_in_type1_a1_policyset(policy_set, cons, seniority_year, scheduled_hours):
    number = ((cons*scheduled_hours)/5) + ((seniority_year*scheduled_hours)/5)
    for x in policy_set['timeOffBalanceEventScripts']:
        if x['scriptTarget']['name'] == "Monthly Accrual":
            for y in x['additionalParameters']:
                if y['keyUri'] == "urn:replicon:script-key:parameter:accrual-annual-amount":
                    y['value']['number'] = float(number)
    return null

def set_cap_limit_in_policy_set(policy_set, cap_limit):
    for x in policy_set['timeOffBalanceEventScripts']:
        if x['scriptTarget']['name'] == "Cap Accruals for the Year":
            for y in x['additionalParameters']:
                if y['keyUri'] == "urn:replicon:script-key:parameter:maximum-accrual-amount":
                    y['value']['number'] = float(cap_limit)
    return null

def set_carry_upto_in_policy_set(policy_set, scheduled_hours):
    number = (scheduled_hours/5) * 3
    for x in policy_set['timeOffBalanceEventScripts']:
        if x['scriptTarget']['name'] == "Yearly Carry Over with Expiry":
            for y in x['additionalParameters']:
                if y['keyUri'] == "urn:replicon:script-key:parameter:carry-up-to-amount":
                    y['value']['number'] = number
    return null

def evaluate_seniority_condition(condition, seniority_years):
    """
    Evaluate a seniority condition against the employee's years of service.
    Supported operators:
    - ">=X": Greater than or equal to X years
    - "<=X": Less than or equal to X years
    - ">X": Greater than X years
    - "<X": Less than X years
    Args:
        condition: String like ">=0", ">=7", "<1", ">2", "<=2"
        seniority_years: Employee's years of service (int or float)
    Returns:
        Tuple of (is_match: bool, threshold: float)
        - is_match: Whether the condition is satisfied
        - threshold: The numeric threshold value extracted from condition
    """
    if not condition:
        return False, -1

    condition = condition.strip()

    if condition.startswith(">="):
        threshold = float(condition[2:])
        return seniority_years >= threshold, threshold
    elif condition.startswith("<="):
        threshold = float(condition[2:])
        return seniority_years <= threshold, threshold
    elif condition.startswith(">"):
        threshold = float(condition[1:])
        return seniority_years > threshold, threshold
    elif condition.startswith("<"):
        threshold = float(condition[1:])
        return seniority_years < threshold, threshold

    return False, -1


def get_matching_accrual_entry(timeoff_type_name, seniority_years, accrual_mapper):
    """
    Find the matching entry from the timeoff accrual mapper based on leave type and seniority.
    For leave types with multiple seniority tiers (like Canada_Vacation with <=2, >2, >=3, etc.),
    this function finds the MOST SPECIFIC matching tier for the employee's years of service.
    Matching Logic:
    1. Filter entries by leave_type matching timeoff_type_name
    2. For each matching entry, evaluate the seniority_condition
    3. Return the entry with the highest applicable tier (most specific match)
    Args:
        timeoff_type_name: Name of the timeoff type (e.g., "USA _Vacation", "Canada_Vacation")
        seniority_years: Employee's years of service
        accrual_mapper: List of accrual policy entries
    Returns:
        Dictionary with the matching accrual entry, or None if no match found
    """
    if not timeoff_type_name or seniority_years is None:
        return None

    seniority_years = float(seniority_years)

    # Find all entries matching the leave type
    matching_entries = [
        entry for entry in accrual_mapper
        if entry.get("leave_type") == timeoff_type_name
    ]

    if not matching_entries:
        return None

    # Evaluate seniority conditions and find the best match
    # For tiered policies, we want the highest applicable tier
    best_match = None
    best_threshold = -1

    for entry in matching_entries:
        condition = entry.get("seniority_condition", "")
        is_match, threshold = evaluate_seniority_condition(condition, seniority_years)

        if is_match and threshold > best_threshold:
            best_threshold = threshold
            best_match = entry

    return best_match


def get_yearly_accrual_rate(seniority_level, timeoff_type_name, accrual_mapper):
    """
    Get the matching accrual entry based on seniority_level configuration.
    Returns:
        Dictionary with the matching accrual entry details, or None if no match
    """
    return get_matching_accrual_entry(timeoff_type_name, seniority_level, accrual_mapper)

def get_type1_a1_policyline(policyset_details, seniority_year, cap_limit, scheduled_hours, cons):
    existing_yearly_entitlement = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Monthly Accrual')
    if existing_yearly_entitlement:
        set_yearly_entitlement_amount_in_type1_a1_policyset(
            policyset_details['policySet'], cons, seniority_year, scheduled_hours)
        
    existing_cap_limit = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Cap Accruals for the Year')
    if existing_cap_limit:
        cap_limit = ((cons*scheduled_hours)/5) + ((seniority_year*scheduled_hours)/5)
        set_cap_limit_in_policy_set(
            policyset_details['policySet'], cap_limit)

def get_type_1a_policyline(policyset_details, accrual_rate_based_on_seniority, cap_limit, scheduled_hours, fte):
    existing_yearly_entitlement = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Monthly Accrual')
    if existing_yearly_entitlement:
        set_yearly_entitlement_amount_in_policy_set(
            policyset_details['policySet'], accrual_rate_based_on_seniority, scheduled_hours, fte)
        
    existing_cap_limit = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Cap Accruals for the Year')
    if existing_cap_limit:
        set_cap_limit_in_policy_set(
            policyset_details['policySet'], cap_limit)

def get_type_1b_policyline(policyset_details, accrual_rate_based_on_seniority, cap_limit, scheduled_hours):
    existing_yearly_entitlement = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Monthly Accrual')
    if existing_yearly_entitlement:
        set_yearly_entitlement_amount_in_policy_set(
            policyset_details['policySet'], accrual_rate_based_on_seniority, scheduled_hours, 5)
        
    existing_cap_limit = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Cap Accruals for the Year')
    if existing_cap_limit:
        set_cap_limit_in_policy_set(
            policyset_details['policySet'], cap_limit)
        
    yearly_carry_over = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Yearly Carry Over with Expiry')
    if yearly_carry_over:
        set_carry_upto_in_policy_set(
            policyset_details['policySet'], scheduled_hours)

def get_type_2a_policyline(policyset_details, accrual_rate_based_on_seniority, scheduled_hours):
    existing_yearly_entitlement = rail.find_first_by_attr_and_get_attr(
        policyset_details['policySet']['timeOffBalanceEventScripts'], 'scriptTarget.name', 'Starting Balance Set To')
    if existing_yearly_entitlement:
        set_starting_balance_amount_in_policy_set(
            policyset_details['policySet'], accrual_rate_based_on_seniority, scheduled_hours)

def get_final_policyset_schedule(
        historical_policies,
        effective_date,
        default_policy,
        oefs,
        timeoff_type,
        config
    ):
    to_type = config.TIMEOFF_TYPE_MAPPER.get(timeoff_type, '')
    mapper_value = get_yearly_accrual_rate(
        oefs['seniority_years'],
        timeoff_type,
        config.ACCRUAL_RATE_MAPPER
    )
    mapper_value = mapper_value or {}
    
    new_policy_lines = []

    for i, policyset_details in enumerate(default_policy):
        if i == 0:
            effective_date_for_policyset = to_datetime(effective_date)
            yearly_accrual_rate = mapper_value.get('yearly_accrual_rate', 0.0)
            cap_limit = mapper_value.get('cap_accruals_for_year_hours', 0.0)

            if to_type == 'type_1a':
                if timeoff_type in ('USA _Vacation', 'Canada_Vacation'):
                    seniority_years = oefs['seniority_years']
                    cons = 15 if timeoff_type == 'Canada_Vacation' else 20
                    get_type1_a1_policyline(policyset_details, seniority_years, cap_limit, oefs['scheduled_hours'], cons)
                else:
                    get_type_1a_policyline(policyset_details, yearly_accrual_rate, cap_limit, oefs['scheduled_hours'], oefs['fte'])

            if to_type == 'type_1b':
                get_type_1b_policyline(policyset_details, yearly_accrual_rate, cap_limit, oefs['scheduled_hours'])

            if to_type in ['type_2a', 'type_2b', 'type_3']:
                get_type_2a_policyline(policyset_details, yearly_accrual_rate, oefs['scheduled_hours'])

            new_policy_lines.append({
                "description": f"Added by Integration on {effective_date_for_policyset.strftime(DATE_FORMAT)}",
                "effectiveDate": rail.get_replicon_date(effective_date_for_policyset),
                "policySet": policyset_details['policySet']
            })

    historical_policies.extend(json.loads(json.dumps(new_policy_lines).replace('"null"', '"effective"').replace(
        '"script"', '"scriptTarget"')))

    return historical_policies

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    policylogs = dag_run.conf['policylogs']

    if policylogs:
        if isinstance(policylogs, list):
            log_artifacts.extend(policylogs)
        elif isinstance(policylogs, str) and policylogs[0] == '[':
            policylogs = literal_eval(policylogs)
            log_artifacts.extend(policylogs)
        else:
            log_artifacts.append(policylogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    def get_log_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"
    
    def get_log_details(user_logs, key):
        message = list(set(map(lambda x: x['properties'].get(key), user_logs)))
        return "; ".join(message)

    final_log_records = []

    users = list(map(lambda x: {
        'user': f"{x['properties'].get('login_name', '')}"
        }, log_records))

    final_data = list({f"{value['user']}": value for value in users}.values())

    #pylint: disable=cell-var-from-loop
    for item in final_data:
        user_logs = list(
            filter(lambda x: 
                   (x['properties'].get('login_name', '') == item['user']), log_records))
        if len(user_logs) > 0:
            first = user_logs[0]
            final_log_records.append({
                "login_name": first['properties']['login_name'],
                "timeoff_type":  get_log_details(user_logs, "timeoff_type"),
                "status": get_log_status(user_logs),
                "details":  get_log_details(user_logs, "details"),
                "ecid": first['ecid'],
            })

    
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    
    return final_log_records
