import json
import pendulum
from datetime import datetime, date, timedelta
from ast import literal_eval
import rail
from guidehouse.workday_user_import.utils.request_payload import get_schedule_policy, get_assigned_timeoff_to_users

null = None

DATE_FORMAT = "%m/%d/%Y"

SEPERATOR = ","

# Mandatory fields for Guidehouse user records
MANDATORY_KEY = [
    'employee_id', 'login_name', 'first_name', 'last_name', 'location',
    'employee_type', 'change_effective_date', 'schedule', 'start_date',
    'user_status', 'company_code', 'cost_center_description', 'financial_system'
]


def get_today_date():
    now = pendulum.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def clean_pipe_string(input_str):
    parts = [part.strip() for part in input_str.split(SEPERATOR)]
    return SEPERATOR.join(parts)


def strip_extra_spaces(value):
    parts = [part.strip() for part in value.split(SEPERATOR)]
    return SEPERATOR.join(parts)


def normalize_separator_value(value):
    if not value:
        return value
    parts = [part.strip() for part in value.split(SEPERATOR)]
    return SEPERATOR.join(parts)


def get_payload_user_data():
    payload_user = rail.load_all_records(rail.result('query_user_data'))
    user_data = payload_user[0]
    user_data['location'] = clean_pipe_string(user_data['location'])
    user_data['employee_type'] = normalize_separator_value(user_data['employee_type'])
    return user_data



def derive_mapper_values(config, user_data):
    location_parts = [p.strip() for p in user_data['location'].split(SEPERATOR)]
    loc1 = location_parts[0] if location_parts else ''
    loc2 = location_parts[1] if len(location_parts) > 1 else ''

    et_parts = [p.strip() for p in user_data['employee_type'].split(SEPERATOR)]
    et1 = et_parts[0] if et_parts else ''
    et2 = et_parts[1] if len(et_parts) > 1 else ''
    et3 = et_parts[2] if len(et_parts) > 2 else ''

    company_code = user_data.get('company_code', '')
    financial_system = user_data.get('financial_system', '')

    raw_time_profile = user_data.get('time_profile_name', '') or ''
    normalized_time_profile = 'NA' if not raw_time_profile \
        or raw_time_profile.upper() in ('NA', 'N/A') else raw_time_profile

    def matches(user_val, mapper_val):
        return mapper_val.lower() == 'all' or user_val == mapper_val

    def matches_financial_system(user_val, mapper_val):
        if isinstance(mapper_val, list):
            return user_val in mapper_val
        return mapper_val.lower() == 'all' or user_val == mapper_val

    matched = [
        row for row in config.USER_SYNC_MAPPER
        if matches(loc1, row['location_level_1'])
        and matches(loc2, row['location_level_2'])
        and matches(company_code, row['company_code'])
        and matches_financial_system(financial_system, row['financial_system'])
        and matches(et1, row['employee_type_level_1'])
        and matches(et2, row['employee_type_level_2'])
        and matches(et3, row['employee_type_level_3'])
        and row['time_profile_name'] == normalized_time_profile
    ]

    if not matched:
        return {}

    return dict(matched[0])


def _is_eligible_by_weekly_hours(weekly_hours, min_val):
    if min_val == '' or min_val is None:
        return True
    if isinstance(min_val, (int, float)):
        return min_val == 0 or weekly_hours >= min_val
    min_str = str(min_val).strip().lower()
    if not min_str or min_str == '0':
        return True
    if min_str.startswith('<'):
        try:
            return weekly_hours < float(min_str[1:])
        except ValueError:
            return True
    try:
        return weekly_hours >= float(min_str)
    except ValueError:
        return True

def get_matched_timeoff_types(config, dag_run):
    location_parts = [p.strip() for p in dag_run.conf['location'].split(SEPERATOR)]
    loc1 = location_parts[0] if location_parts else ''

    et_parts = [p.strip() for p in dag_run.conf['employee_type'].split(SEPERATOR)]
    et1 = et_parts[0] if len(et_parts) > 0 else ''
    et2 = et_parts[1] if len(et_parts) > 1 else ''
    et3 = et_parts[2] if len(et_parts) > 2 else ''
    et4 = et_parts[3] if len(et_parts) > 3 else ''

    schedule = dag_run.conf.get('schedule', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 0

    return list(dict.fromkeys(
        row['time_off_type_name']
        for row in config.TIMEOFF_MAPPER
        if row['country'].lower() == loc1.lower()
        and row['employee_type_hierarchy_level_1'] == et1
        and row['employee_type_hierarchy_level_2'] == et2
        and row['employee_type_hierarchy_level_3'] == et3
        and row['employee_type_hierarchy_level_4'] == et4
        and _is_eligible_by_weekly_hours(weekly_hours, row['min_weekly_scheduled_hours_for_eligibility'])
    ))


def get_selected_timeoff_uris():
    uri_by_name = {t['displayText']: t['uri'] for t in rail.result('get_enabled_timeoff_types')}
    return {
        'holiday_uri': uri_by_name.get('Holiday', ''),
        'floating_holiday_uri': uri_by_name.get('[USA] Floating Holiday', ''),
        'sick_uri': uri_by_name.get('[USA] Sick', ''),
        'can_floating_holiday_uri': uri_by_name.get('[CAN] Floating Holiday', ''),
        'gbr_floating_holiday_uri': uri_by_name.get('[GBR] Floating Holiday', ''),
        'can_sick_uri': uri_by_name.get('[CAN] Sick', ''),
    }


def get_non_eligible_timeoff_types(config, dag_run, current_assigned_types):
    currently_eligible = get_matched_timeoff_types(config, dag_run)
    loa_excluded = config.LOA_EXCLUDED_TIMEOFF_TYPES
    return [t for t in current_assigned_types if t not in currently_eligible and t not in loa_excluded]


def _build_zero_item(dag_run, user_data, type_uri, type_name, effective_date):
    return {
        'useruri': dag_run.conf['useruri'],
        'timeoffuri': type_uri,
        'timeoff_type_name': type_name,
        'effective_date': effective_date,
        'policyset': get_existing_policy_schedule(user_data, type_uri, effective_date),
        'user_log': dag_run.conf['user_log'],
        'starting_balance_script_uri': dag_run.conf['starting_balance_script_uri'],
    }


def get_zero_timeoff_items(config, dag_run):
    """
    Build the list of time-off policy items to zero out.

    - Termination (end_date in past): zero all assigned non-LOA-excluded types at end_date.
    - Non-eligible (schedule change): zero types no longer eligible at change_effective_date.
    - Otherwise: return [] (no zeroing needed).

    Each item: {useruri, timeoffuri, effective_date, policyset, user_log, starting_balance_script_uri}.
    """
    user_data = rail.result('get_user_data')[0]
    policies_by_type = (user_data.get('timeOffTypePolicySummary') or {}).get('policiesByTimeOffType') or []
    loa_excluded = config.LOA_EXCLUDED_TIMEOFF_TYPES

    if if_end_date_in_past(dag_run):
        effective_date = dag_run.conf['end_date']
        items = []
        for policy_entry in policies_by_type:
            type_name = (policy_entry.get('timeOffType') or {}).get('displayText', '')
            type_uri = (policy_entry.get('timeOffType') or {}).get('uri', '')
            if not type_uri or type_name in loa_excluded:
                continue
            items.append(_build_zero_item(dag_run, user_data, type_uri, type_name, effective_date))
        return items

    non_eligible_names = rail.result('get_non_eligible_types') or []
    if not non_eligible_names:
        return []

    effective_date = dag_run.conf['change_effective_date']
    uri_by_name = {}
    for entry in policies_by_type:
        timeoff_type = entry.get('timeOffType') or {}
        uri = timeoff_type.get('uri', '')
        for key in ('name', 'displayText'):
            if timeoff_type.get(key):
                uri_by_name[timeoff_type[key]] = uri
    items = []
    for type_name in non_eligible_names:
        type_uri = uri_by_name.get(type_name)
        if not type_uri:
            continue
        items.append(_build_zero_item(dag_run, user_data, type_uri, type_name, effective_date))
    return items


def _get_intl_floating_holiday_balance(dag_run):
    """
    Shared balance helper for [CAN] and [GBR] Floating Holiday.
    Full_time: 8h fixed. Part_time: prorated as (weekly_hours / 40) * 8.
    No hire-month cutoff (unlike [USA] Floating Holiday).
    Used for both add_user (starting balance) and update_user (schedule change entitlement).
    """
    schedule = dag_run.conf.get('schedule', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 0.0

    return round((weekly_hours / 40) * 8.0, 2)


def _get_floating_holiday_starting_balance(dag_run):
    schedule = dag_run.conf.get('schedule', '') or ''
    start_date_str = dag_run.conf.get('start_date', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 0.0

    full_time_balance = 16.0
    if start_date_str:
        hire_date = datetime.strptime(start_date_str, DATE_FORMAT)
        if hire_date.month >= 10:
            full_time_balance = 8.0

    return round((weekly_hours / 40) * full_time_balance, 2)


def _get_floating_holiday_schedule_change_entitlement(dag_run):
    schedule = dag_run.conf.get('schedule', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 0.0

    return round((weekly_hours / 40) * 16.0, 2)


def _get_remaining_weeks_in_year(start_date, week_start_day=0):
    year_end = date(start_date.year, 12, 31)
    days_since_week_start = (start_date.weekday() - week_start_day) % 7
    week_start = start_date - timedelta(days=days_since_week_start)
    total_days = (year_end - week_start).days + 1
    return min(total_days // 7, 52)


def _get_sick_leave_starting_balance(dag_run):
    schedule = dag_run.conf.get('schedule', '') or ''
    start_date_str = dag_run.conf.get('start_date', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 0.0

    annual_hours = round((weekly_hours / 40) * 80.0, 2)

    if not start_date_str:
        return annual_hours

    start_date = datetime.strptime(start_date_str, DATE_FORMAT).date()
    remaining_weeks = _get_remaining_weeks_in_year(start_date)
    return round(annual_hours * (remaining_weeks / 52), 2)


def _get_sick_leave_schedule_change_entitlement(dag_run):
    schedule = dag_run.conf.get('schedule', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 0.0

    return round((weekly_hours / 40) * 80.0, 2)


def _get_can_sick_leave_starting_balance(dag_run):
    return _get_sick_leave_starting_balance(dag_run)


def _get_can_sick_leave_schedule_change_entitlement(dag_run):
    return _get_sick_leave_schedule_change_entitlement(dag_run)


def _get_holiday_entitlement(config, dag_run):
    location_parts = [p.strip() for p in dag_run.conf.get('location', '').split(SEPERATOR)]
    loc1 = location_parts[0].lower() if location_parts else ''
    loc2 = location_parts[1].lower() if len(location_parts) > 1 else ''
    loc3 = location_parts[2].lower() if len(location_parts) > 2 else ''

    schedule = dag_run.conf.get('schedule', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 40.0

    max_hours = None
    for row in config.HOLIDAY_ENTITLEMENT_MAPPER:
        if row['loc1'] != loc1:
            continue
        if row['exclude_loc2'] and loc2 == row['exclude_loc2']:
            continue
        if row['loc2'] != 'all' and loc2 != row['loc2']:
            continue
        if row['loc3'] is not None and loc3 != row['loc3']:
            continue
        max_hours = row['max_hours']
        break

    if max_hours is None:
        return 0.0

    if weekly_hours >= 40:
        return float(max_hours)
    return round((weekly_hours / 40) * max_hours, 2)

def _get_holiday_max_limit(config, dag_run):
    location_parts = [p.strip() for p in dag_run.conf.get('location', '').split(SEPERATOR)]
    loc1 = location_parts[0].lower() if location_parts else ''
    loc2 = location_parts[1].lower() if len(location_parts) > 1 else ''
    loc3 = location_parts[2].lower() if len(location_parts) > 2 else ''

    max_hours = 0.0
    for row in config.HOLIDAY_ENTITLEMENT_MAPPER:
        if row['loc1'] != loc1:
            continue
        if row['exclude_loc2'] and loc2 == row['exclude_loc2']:
            continue
        if row['loc2'] != 'all' and loc2 != row['loc2']:
            continue
        if row['loc3'] is not None and loc3 != row['loc3']:
            continue
        max_hours = row['max_hours']
        break
    return round(max_hours, 2)

def get_existing_policy_schedule(user_data_record, timeoff_type_uri, change_effective_date):
    policies_by_type = (user_data_record.get('timeOffTypePolicySummary') or {}).get('policiesByTimeOffType') or []
    policy_set_schedule = []
    for entry in policies_by_type:
        if entry.get('timeOffType', {}).get('uri') == timeoff_type_uri:
            policy_set_schedule = entry.get('policySetSchedule') or []
            break
    effective_date = datetime.strptime(change_effective_date, DATE_FORMAT)
    return [
        p for p in policy_set_schedule
        if datetime(day=p['effectiveDate']['day'], month=p['effectiveDate']['month'], year=p['effectiveDate']['year']) < effective_date
    ]


def _normalize_policyset(user_policysetschedule):
    return json.loads(
        json.dumps(user_policysetschedule, ensure_ascii=False)
        .replace('"null"', '"effective"')
        .replace('"script"', '"scriptTarget"')
    )


def get_final_policyset(config,dag_run, default_global_policy, user_policysetschedule, balance=None, timeoff_type_name='', action='add_user'):
    existing_schedule = _normalize_policyset(user_policysetschedule)

    default_policyset_for_0_offset = rail.find_first_by_attr_and_get_attr(
        default_global_policy, 'startOffset.offsetValue', 0, 'policySet'
    )

    if default_policyset_for_0_offset is None:
        return existing_schedule

    starting_balance = balance if balance is not None else 0.0

    starting_balance_with_0 = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": 0.0}}
    )
    modified_starting_balance = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": starting_balance}}
    )

    default_annual_amount = 0.0
    for script_entry in default_policyset_for_0_offset.get('timeOffBalanceEventScripts', []):
        for param in script_entry.get('additionalParameters', []):
            if param.get('keyUri') == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                default_annual_amount = float(param['value']['number'])
                break

    starting_annual_amount = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": default_annual_amount}}
    )
    if timeoff_type_name == 'Holiday':
        annual_amount = _get_holiday_entitlement(config, dag_run)
    elif timeoff_type_name == '[USA] Sick':
        annual_amount = _get_sick_leave_schedule_change_entitlement(dag_run)
    elif timeoff_type_name == '[CAN] Sick':
        annual_amount = _get_can_sick_leave_schedule_change_entitlement(dag_run)
    elif timeoff_type_name in ('[CAN] Floating Holiday', '[GBR] Floating Holiday'):
        annual_amount = _get_intl_floating_holiday_balance(dag_run)
    elif timeoff_type_name == '[USA] Floating Holiday':
        annual_amount = _get_floating_holiday_schedule_change_entitlement(dag_run)
    else:
        annual_amount = default_annual_amount
    modified_annual_amount = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": annual_amount}}
    )

    policyset_json = json.dumps(default_policyset_for_0_offset, ensure_ascii=False)

    if timeoff_type_name == 'Holiday':
        max_limit = _get_holiday_max_limit(config, dag_run)

        default_max_limit = 0.0
        for script_entry in default_policyset_for_0_offset.get('timeOffBalanceEventScripts', []):
            for param in script_entry.get('additionalParameters', []):
                if param.get('keyUri') == 'urn:replicon:script-key:parameter:daily-maximum-balance-amount':
                    default_max_limit = float(param['value']['number'])
                    break

        max_limit_balance = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {"number": default_max_limit}}
        )
        modified_max_limit_balance = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {"number": max_limit}}
        )
        policyset_json = policyset_json.replace(max_limit_balance, modified_max_limit_balance)

    if timeoff_type_name in ('[USA] Sick', '[CAN] Sick'):  # both share same reset-balance-amount logic
        default_reset_amount = 0.0
        for script_entry in default_policyset_for_0_offset.get('timeOffBalanceEventScripts', []):
            for param in script_entry.get('additionalParameters', []):
                if param.get('keyUri') == 'urn:replicon:script-key:parameter:reset-balance-amount':
                    default_reset_amount = float(param['value']['number'])
                    break

        starting_reset_amount = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {"number": default_reset_amount}}
        )
        modified_reset_amount = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {"number": annual_amount}}
        )
        policyset_json = policyset_json.replace(starting_reset_amount, modified_reset_amount)

    policyset_to_add = json.loads(
        policyset_json
        .replace(starting_balance_with_0, modified_starting_balance)
        .replace(starting_annual_amount, modified_annual_amount)
        .replace('"null"', '"effective"')
        .replace('"script"', '"scriptTarget"')
    )

    effective_date_str = dag_run.conf.get('start_date', '')
    if action == 'update_user':
        effective_date_str = dag_run.conf.get('change_effective_date')
    effective_date = datetime.strptime(effective_date_str, DATE_FORMAT)

    existing_schedule.append({
        "description": f"Effective on - {effective_date_str}",
        "effectiveDate": {"year": effective_date.year, "month": effective_date.month, "day": effective_date.day},
        "policySet": policyset_to_add
    })

    return existing_schedule


def get_zero_balance_policyset(dag_run, user_policysetschedule):
    """
    Build a policy set schedule with a zero starting balance at effective_date.

    Normalizes existing entries (null->effective, script->scriptTarget) then
    appends a new Starting Balance Set To entry with amount=0.0.

    Uses dag_run.conf keys: effective_date, starting_balance_script_uri.
    Uses user_policysetschedule: existing filtered entries from conf.
    """
    existing_schedule = _normalize_policyset(user_policysetschedule)

    effective_date_str = dag_run.conf["effective_date"]
    effective_date = datetime.strptime(effective_date_str, DATE_FORMAT)

    existing_schedule.append({
        "description": f"Effective on - {effective_date_str}",
        "effectiveDate": {"year": effective_date.year, "month": effective_date.month, "day": effective_date.day},
        "policySet": {
            "timeOffBalanceEventScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {"number": 0.0}
                }],
                "scriptTarget": {
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To",
                    "uri": dag_run.conf["starting_balance_script_uri"]
                }
            }]
        }
    })

    return existing_schedule


def get_adjusted_balance(time_taken, new_entitlement):
    return max(0.0, float(new_entitlement) - float(time_taken))


def is_schedule_updated(dag_run):
    current_schedule = get_schedule_policy(dag_run, "update_user", dag_run.conf.get('change_effective_date'))
    if not current_schedule:
        return True
    return dag_run.conf.get('schedule_uri', '') != current_schedule['uri']


def is_timeoff_recalculation_needed(config, dag_run, timeoff_type_name=None, action='update_user'):
    """Return True if time-off policies need recalculation.

    For 'add_user': returns True only if timeoff_type_name is in the user's
    incoming eligible types.

    For 'update_user', triggers recalculation in two cases:
    - Work schedule URI changed (e.g. FT → PT): returns True if timeoff_type_name
      is in incoming eligible types. Types the user is LOSING are excluded here —
      they are handled separately by zero_non_eligible_timeoff_types.
    - No schedule change: uses bidirectional set comparison (excluding LOA admin
      types) to detect gains (type newly eligible) or losses (type no longer
      eligible) due to location/employee-type change.

    When called with timeoff_type_name=None (e.g. from process_update_users to
    decide whether to enter the task group at all), the schedule-change path is
    skipped and only the bidirectional comparison is used.
    """
    incoming_eligible = set(get_matched_timeoff_types(config, dag_run))
    if action == 'add_user':
        if timeoff_type_name and timeoff_type_name in incoming_eligible:
            return True
        return False
    loa_excluded = set(config.LOA_EXCLUDED_TIMEOFF_TYPES)
    current_names = get_assigned_timeoff_to_users(dag_run, action)['enabled']
    if is_schedule_updated(dag_run):
        if not timeoff_type_name:
            return True
        if timeoff_type_name in incoming_eligible:
            return True
    current_assigned = current_names - loa_excluded
    incoming_eligible = incoming_eligible - loa_excluded
    if not timeoff_type_name:
        return incoming_eligible != current_assigned
    if timeoff_type_name in incoming_eligible and timeoff_type_name not in current_assigned:
        return True
    return False


def should_assign_timeoff_type(config, dag_run, timeoff_type_name, action):
    """Return True if timeoff_type_name should be assigned or re-assigned to the user.

    Returns True when any of the following hold:
    1. action is 'add_user' — first-time policy setup.
    2. User is being reactivated: userDetails.isEnabled is false in Replicon and
       incoming user_status is 'Active' — policies need to be (re-)created.
    3. The type was previously disabled (isTimeOffAllowedAgainstThisTimeOffType=false)
       — needs to be re-enabled with a fresh policy.
    4. The type was not present at all in the user's current policiesByTimeOffType
       — newly eligible, must be assigned for the first time.
    """

    if action == 'add_user':
        return True

    user_data = rail.result('get_user_data')[0]

    # Condition 2: user reactivation
    if dag_run.conf.get('user_status', '') == 'Active':
        current_status = (user_data.get('userDetails') or {}).get('isEnabled')
        if current_status in ('false', False):
            return True

    # Conditions 3 and 4: type was disabled or not yet assigned
    assigned = get_assigned_timeoff_to_users(dag_run, action)
    return timeoff_type_name not in assigned['enabled']


def get_licences_to_be_assigned(config):
    resp = []
    for license in config.licenses:
        if license == "TOE":
            resp.append("urn:replicon-saas:product:time-off-enterprise")
        if license == "WFM":
            resp.append("urn:replicon-saas:product:wfm-enterprise")
        if license == "Polaris PSA":
            resp.append("urn:replicon-saas:product:psm-enterprise-2")
    return resp


def get_all_permissionseturis():
    permissionsets = []
    permissionsets.append({
        'name': "Employee",
        'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_permission_sets'), 'displayText', "Employee", 'uri')
    })
    return permissionsets


def _get_all_records(artifact):
    return rail.load_all_records(artifact)


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_process_users_conf(item, config):
    selected_uris = rail.result('get_selected_timeoff_uris')
    return {
        'licences': get_licences_to_be_assigned(config),
        'permissionsetdetails': get_all_permissionseturis(),
        "employee_id": item['employee_id'],
        "replicon_location_details": rail.write_json_artifact(rail.result('get_location_details')),
        "replicon_usertypes_details": rail.write_json_artifact(rail.result('get_employeetype_groups_data')),
        "replicon_division_details": rail.write_json_artifact(rail.result('get_enabled_divisions')),
        "replicon_costcenter_details": rail.write_json_artifact(rail.result('get_all_costcenters')),
        "replicon_servicecenter_details": rail.write_json_artifact(rail.result('get_all_servicecenters')),
        "replicon_user_udfs": rail.result('get_user_customfields'),
        "replicon_permission_sets": rail.write_json_artifact(rail.result('get_permission_sets')),
        "replicon_payrules": rail.write_json_artifact(rail.result('get_all_payrule_scripts')),
        "replicon_policy_sets": rail.write_json_artifact(rail.result('get_all_policy_sets')),
        "replicon_ts_approval_paths": rail.write_json_artifact(rail.result('get_timesheet_approval_paths')),
        "replicon_timeoff_approval_paths": rail.write_json_artifact(rail.result('get_timeoff_approval_paths')),
        "replicon_all_timezones": rail.write_json_artifact(rail.result('get_all_timezones')),
        "replicon_office_schedule": rail.write_json_artifact(rail.result('get_updated_all_office_schedule')),
        "replicon_user_status_dropdown": rail.result('get_all_user_status_dropdowns'),
        "replicon_ts_period_list": rail.write_json_artifact(rail.result('get_all_timesheet_period_list')),
        "replicon_holiday_calendars": rail.write_json_artifact(rail.result('get_all_holiday_calendars')),
        "replicon_enabled_timeoff_types": rail.write_json_artifact(rail.result('get_enabled_timeoff_types')),
        "replicon_activity_uris": rail.result('get_all_activity_uris'),
        'supervisor_log': rail.result('process_supervisor_log'),
        'default_policyline_holiday': rail.write_json_artifact(rail.result('get_default_policyline_holiday')),
        'default_policyline_floating_holiday': rail.write_json_artifact(rail.result('get_default_policyline_floating_holiday')),
        'default_policyline_sick': rail.write_json_artifact(rail.result('get_default_policyline_sick')),
        'default_policyline_can_floating_holiday': rail.write_json_artifact(rail.result('get_default_policyline_can_floating_holiday')),
        'default_policyline_gbr_floating_holiday': rail.write_json_artifact(rail.result('get_default_policyline_gbr_floating_holiday')),
        'default_policyline_can_sick': rail.write_json_artifact(rail.result('get_default_policyline_can_sick')),
        'holiday_uri': selected_uris['holiday_uri'],
        'floating_holiday_uri': selected_uris['floating_holiday_uri'],
        'sick_uri': selected_uris['sick_uri'],
        'can_floating_holiday_uri': selected_uris['can_floating_holiday_uri'],
        'gbr_floating_holiday_uri': selected_uris['gbr_floating_holiday_uri'],
        'can_sick_uri': selected_uris['can_sick_uri'],
        'starting_balance_script_uri': rail.result('get_all_scripts_time_off_balance_event_script')['starting_balance_set_to'],
    }




def get_process_new_users_conf(config, dag_run):
    user_payload_data = rail.result('get_user_payload_data')
    mapper_data = derive_mapper_values(config,user_payload_data)
    location = user_payload_data['location'].split(SEPERATOR)

    def timezone_details():
        country = location[0].lower()
        province = location[1].lower() if len(location) > 1 else ''
        tz = ''
        uri = ''
        for item in config.TIMEZONE_MAPPER:
            if item['country'].lower() != country:
                continue
            item_province = item.get('province', '').lower()
            if item_province:
                if item_province == province:
                    tz = item['time_zone']
                    break
            else:
                tz = item['time_zone']  # country-level fallback, keep searching for province match
        if tz:
            for rec in _get_all_records(dag_run.conf['replicon_all_timezones']):
                if rec['displayText'] == tz:
                    uri = rec['uri']
                    break
        return {'tz': tz, 'uri': uri}

    def get_holiday_calendar_uri():

        country = location[0].lower()
        state = location[1].lower() if len(location) > 1 else ''
        city = location[2].lower() if len(location) > 2 else ''

        # India and Germany: use city/state level (L3)
        from guidehouse.workday_user_import.config import LEVEL3_HOLIDAY_CALENDAR_FOR
        if country in LEVEL3_HOLIDAY_CALENDAR_FOR:
            search_with = city
        else:
            search_with = country
        for rec in _get_all_records(dag_run.conf['replicon_holiday_calendars']):
            if rec['displayText'].lower() == search_with:
                return rec['uri']
        return ''

    def get_default_work_location():
        from guidehouse.workday_user_import.config import LEVEL2_DEFAULT_ACTIVITY_COUNTRIES, NON_EXEMPT_EMPLOYEE_TYPE_LEVEL2
        country = location[0].lower()
        search_term = country
        employee_type_parts = user_payload_data['employee_type'].split(SEPERATOR)
        employee_level2 = employee_type_parts[1] if len(employee_type_parts) > 1 else ''
        if employee_level2.lower() not in ['non-exempt', 'non_exempt']: return ''
        # USA/Canada Non-Exempt → use L2 as default activity
        if country in [c.lower() for c in LEVEL2_DEFAULT_ACTIVITY_COUNTRIES] and employee_level2 == NON_EXEMPT_EMPLOYEE_TYPE_LEVEL2:
            search_term = location[1] if len(location) > 1 else ''
        return search_term

    holiday_calendar_uri = get_holiday_calendar_uri()
    tz = timezone_details()

    timesheet_period = mapper_data['timesheet_period'] if mapper_data else ''
    timesheet_period_uri = rail.find_first_by_attr_and_get_attr(
        _get_all_records(dag_run.conf['replicon_ts_period_list']), 'name', timesheet_period, 'uri'
    ) if timesheet_period else ''
    return {
        **user_payload_data,
        **dag_run.conf,
        **{
            'holiday_calander_uri': holiday_calendar_uri,
            'timezone': tz['tz'],
            'pay_rule': mapper_data['pay_rule'] if mapper_data else '',
            'punch_policy': mapper_data['punch_policy'] if mapper_data and mapper_data['punch_policy'] != 'NA' else '',
            'work_week': mapper_data['work_week'] if mapper_data else '',
            'work_week_uri': rail.find_first_by_attr_and_get_attr(config.WORKWEEK_MAPPER, 'value', mapper_data[
                'work_week'].split()[0].lower(), 'uri') if mapper_data else '',
            'activities': dag_run.conf['replicon_activity_uris'],
            'timezoneuri': tz['uri'],
            'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_permission_sets']), 'displayText', "Supervisor", 'uri'),
            'payrule_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_payrules']), 'displayText', mapper_data['pay_rule'], 'uri') if mapper_data else '',
            'country_name': location[0],
            'location_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_location_details']), 'fullpath', user_payload_data['location'].lower(), 'uri'),
            'employee_type_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_usertypes_details']), 'fullpath', user_payload_data['employee_type'], 'uri'),
            'costcenter_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_division_details']), 'name', user_payload_data['cost_center_description'], 'uri'),
            'company_code_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_costcenter_details']), 'name', user_payload_data['company_code'], 'uri'),
            'servicecenter_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_servicecenter_details']), 'name', user_payload_data['financial_system'], 'uri'),
            'schedule_uri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf['replicon_office_schedule']), 'displayText', user_payload_data['schedule'], 'uri'),
            'user_status_value_uri': rail.find_first_by_attr_and_get_attr(
                dag_run.conf['replicon_user_status_dropdown'], 'displayText', user_payload_data['user_status'], 'uri'),
            'default_work_location': get_default_work_location(),
            'timesheetperiod': timesheet_period,
            'timesheet_period_uri': timesheet_period_uri,
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf["replicon_policy_sets"]), 'displayText', mapper_data[
                    'timesheet_template'], 'uri') if mapper_data else '',
            'timesheettemplate': mapper_data['timesheet_template'] if mapper_data else '',
            'timesheetapprovalpath': mapper_data['timesheet_approval_path'] if mapper_data else '',
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf["replicon_ts_approval_paths"]), 'displayText', mapper_data[
                    'timesheet_approval_path'], 'uri') if mapper_data else '',
            'timeofftemplateuri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf["replicon_policy_sets"]), 'displayText', mapper_data[
                    'time_off_template'], 'uri') if mapper_data else '',
            'timeofftemplate': mapper_data['time_off_template'] if mapper_data else '',
            'timeoffapprovalpath': mapper_data['time_off_approval_path'] if mapper_data else '',
            'timeoffapprovalpathuri': rail.find_first_by_attr_and_get_attr(
                _get_all_records(dag_run.conf["replicon_timeoff_approval_paths"]), 'displayText', mapper_data[
                    'time_off_approval_path'], 'uri') if mapper_data else '',
            'mapper_data': mapper_data,
            'user_log': rail.result('process_user_log'),
        }
    }


def get_supervisor_permission_uri():
    return rail.find_first_by_attr_and_get_attr(rail.result('get_permission_sets'), 'displayText', "Supervisor", 'uri')


def get_add_user_message():
    exception_logs = rail.result('add_new_user', 'exception_logs')
    if get_task_state('log_user_supervisor_same') == 'success':
        return "Employee and Supervisor is same;" + rail.smartjoin_by_delim(exception_logs, ";")
    if not exception_logs:
        return "User Added Successfully"
    return "User Partially Added;" + rail.smartjoin_by_delim(exception_logs, ";")


def get_add_user_severity():
    if get_task_state('log_user_supervisor_same') == 'success' \
            or rail.result('add_new_user', 'exception_logs'):
        return 'Exception'
    return 'Success'


def validate_enddate(dag_run):
    if dag_run.conf['start_date'] and dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) >= datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)
    return False


def if_end_date_in_past(dag_run):
    current = pendulum.now().strftime(DATE_FORMAT)
    # current = '09/30/2026'  # Hardcoded for testing purposes
    if not dag_run.conf['end_date']:
        return False
    return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) < datetime.strptime(current, DATE_FORMAT)


def get_process_update_users_conf(config, dag_run):
    return {
        **get_process_new_users_conf(config, dag_run),
        **{
            'useruri': rail.result('get_user_by_empl_id')[0]['userDetails']['uri'],
            'user_data': rail.write_json_artifact(rail.result('get_user_by_empl_id'))
        }
    }


def get_update_user_message():
    exception_logs = rail.result('update_existing_user', 'exception_logs')
    if get_task_state('log_user_supervisor_same') == 'success':
        return "Employee and Supervisor is same;" + rail.smartjoin_by_delim(exception_logs, ";")
    if not exception_logs:
        return "User Updated Successfully"
    return "User Partially Updated;" + rail.smartjoin_by_delim(exception_logs, ";")


def get_update_user_severity():
    if get_task_state('log_user_supervisor_same') == 'success' \
            or rail.result('update_existing_user', 'exception_logs'):
        return 'Exception'
    return 'Success'


def get_out_of_scope_location(user_data, locations):
    if not locations:
        return False
    location_parts = user_data['location'].split(SEPERATOR)
    if location_parts[0].lower() in [loc.lower() for loc in locations]:
        return True
    return False


def can_user_profile_enable(dag_run):
    can_enable = not bool(rail.result('get_user_data')[0]['userDetails']['isEnabled']) and not if_end_date_in_past(dag_run)
    can_enable = can_enable and bool(rail.result('get_direct_reports_for_user'))
    return can_enable


def get_supervisor_data_with_manager_id(dag_run):
    return {
        "users": [
            {
                "uri": null,
                "loginName": null,
                "employeeId": dag_run.conf['manager'],
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
            rail.result('search_supervisor_in_replicon')['loginname'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName']:
        return False
    return True


def get_supervisor_message(action, dag_run):
    exception_log = dag_run.conf['exception_logs'] if dag_run.conf.get('exception_logs') else []
    if get_task_state('log_supervisor_not_present') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + \
            ',Supervisor not present in replicon;' + rail.smartjoin_by_delim(exception_log, ";")
    return f"""User {('Added Successfully' if action == 'add' else 'Updated Successfully')
        if not exception_log else ('Partially Added,' if action == 'add' else 'Partially Updated,') + rail.smartjoin_by_delim(exception_log, ";")}"""


def get_supervisor_severity():
    if get_task_state('log_supervisor_not_present') == 'success':
        return 'Exception'
    return 'Success'


def load_records(log_artifact):
    return rail.load_all_records(log_artifact)


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        elif isinstance(userlogs, str) and userlogs[0] == '[':
            userlogs = literal_eval(userlogs)
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    def get_log_status(user_logs):
        available_status = [log['status'] for log in user_logs]
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"

    flat_records = list(map(lambda log: {
        **{'ecid': log['ecid']},
        **dict(log['properties'].items()),
    }, log_records))

    unique_employee_ids = list({r['employeeid']: r for r in flat_records}.keys())

    final_log_records = []
    # pylint: disable=cell-var-from-loop
    for emp_id in unique_employee_ids:
        user_logs = [r for r in flat_records if r['employeeid'] == emp_id]
        first = user_logs[0]
        final_log_records.append({
            **first,
            'status': get_log_status(user_logs),
            'details': '; '.join(list(set(r.get('details', '') for r in user_logs if r.get('details')))),
        })

    rail.set_result(key="error_record_count", val=len(list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="skipped_record_count", val=len(list(filter(lambda x: x['status'] == 'Skipped', final_log_records))))

    return final_log_records


def get_effective_division_or_user(group_schedule, group_name):
    if not group_schedule:
        return null
    today = pendulum.now().date()
    effective_group = None
    latest_date = None
    for item in group_schedule:
        date_info = item.get("effectiveDate")
        if date_info is None:
            item_date = pendulum.datetime(1900, 1, 1).date()
        else:
            item_date = pendulum.date(date_info["year"], date_info["month"], date_info["day"])
        if item_date <= today and (latest_date is None or item_date > latest_date):
            latest_date = item_date
            effective_group = item[group_name]
    return effective_group


def get_zero_indx_value(strng):
    return strng.split(SEPERATOR)[0]


def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key in MANDATORY_KEY:
        if not item[payload_key]:
            payload_key = ' '.join(word.capitalize() for word in payload_key.split('_'))
            missing_fields.append(payload_key)
    log_msg = rail.smartjoin_by_delim(missing_fields, ";")
    log_msg = f"mandatory field(s) {log_msg} is not present in payload"
    return log_msg


