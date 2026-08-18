from datetime import datetime, date as date_type, timedelta
import rail
import json
from dateutil.relativedelta import relativedelta
from rail.lib.ecid import get_dagrun_ecid
from calendar import monthrange
import json
from datetime import datetime, timedelta

def get_input_validationlog(dag_run):
    exception_list = []
    if not dag_run.conf['userid']:
        exception_list.append('Login name not present')
    if not dag_run.conf['firstname']:
        exception_list.append('First_Name not present')
    if not dag_run.conf['lastname']:
        exception_list.append('Last_Name not present')
    if not dag_run.conf['hiredate']:
        exception_list.append('Hire date not present')
    if not dag_run.conf['emailaddress']:
        exception_list.append('Email_Address not present')
    if not dag_run.conf['exemptionstatus']:
        exception_list.append('Excemption Status not present')
    if not dag_run.conf['workertype']:
        exception_list.append('Worker type not present')
    if not dag_run.conf['location']:
        exception_list.append('Department (location) not present')
    if not dag_run.conf['active']:
        exception_list.append('Employee status not present')
    if not dag_run.conf['manager_id']:
        exception_list.append('Manager ID not present')
    if not dag_run.conf['country']:
        exception_list.append('Country not present')

    if len(exception_list) > 0:
        return {
            'exc_present': True,
            'exc_value': ','.join(exception_list)
        }
    return {
        'exc_present': False,
        'exc_value': ''
    }


def get_details_for_employeetype_and_departmentygrpuri_not_exist(dag_run):
    details = ''
    if not (rail.result('get_required_employeetype_uri_40')):
        details = details + ";" + \
            'User not created, since Employee type group does not exist in Replicon or is disabled'
    if not (dag_run.conf['departmentgroupuri']):
        details = details + ";" + \
            'User not created, since Department (location)  does not exist in Replicon or is disabled'
    return details


def split_date_string(date_str, split_type='string'):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    if split_type == 'datetime':
        return {
            "day": date.day,
            "month": date.month,
            "year": date.year
        }
    if split_type == 'int':
        return {
            "day": int(date.strftime("%d")),
            "month": int(date.strftime("%m")),
            "year": int(date.strftime("%Y"))
        }

    return {
        "day": date.strftime("%d"),
        "month": date.strftime("%m"),
        "year": date.strftime("%Y")
    }


def is_valid_date(value, fmt="%Y-%m-%d"):
    """Return True only if `value` is a non-empty string parseable as a date in `fmt`.

    Guards the fixed-term rehire flow: yoss (continuous service date) can arrive blank or
    as a non-date value (e.g. "0") when the source continuous-service-date is missing. The
    recipe's #9 present-guard assumes a valid date and Workato's `.to_date` is lenient, but
    Python's strict `strptime` raises `ValueError` and fails the task. Used to route a
    blank/non-date yoss to the No path (graceful skip) instead of crashing.
    """
    if not value:
        return False
    try:
        datetime.strptime(str(value), fmt)
        return True
    except (ValueError, TypeError):
        return False


def get_userdata_list_for_managerid(response, dag_run):
    if list(filter(lambda x: 'textValue' in x['cells'][0] and x['cells'][0]['textValue'] == dag_run.conf['manager_id'], response['rows'])):
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['manager_id'], list(map(
            lambda d: {
                'uri': d['cells'][1]['uri'],
                'loginname': d['cells'][1]['textValue'],
                'managerid_txt': d['cells'][0]
            }, response['rows']))))
    return []

def search_userdata_list_for_managerid(response, dag_run):
    if list(filter(lambda x: 'textValue' in x['cells'][0] and x['cells'][0]['textValue'] == dag_run.conf['supervisorloginname'], response['rows'])):
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['supervisorloginname'], list(map(
            lambda d: {
                'uri': d['cells'][1]['uri'],
                'loginname': d['cells'][1]['textValue'],
                'managerid_txt': d['cells'][0]
            }, response['rows']))))
    return []

def validate_hiredate_startdate(dag_run):
    return bool(datetime.strptime(str(rail.result('get_user_data_14')[0]['userDetails']['employmentDateRange']['startDate']['year']) + '-' + str(
        rail.result('get_user_data_14')[0]['userDetails']['employmentDateRange']['startDate']['month']) + '-' + str(
        rail.result('get_user_data_14')[0]['userDetails']['employmentDateRange']['startDate']['day']), "%Y-%m-%d") == datetime.strptime(
        dag_run.conf['hiredate'], "%Y-%m-%d"))

def validate_terminationdate_enddate(dag_run):
    # Recipe #35: update when termination date is present AND differs from the
    # existing end date (01/01/2099 when the user has no end date).
    enddate = datetime.strptime('2099-01-01', "%Y-%m-%d")
    userend_date = rail.result('get_user_data_14')[
        0]['userDetails']['employmentDateRange']['endDate']
    if dag_run.conf['terminationdate']:
        if userend_date and 'day' in userend_date:
            enddate = datetime.strptime(
                str(userend_date['year']) + '-' + str(userend_date['month']) + '-' + str(userend_date['day']), "%Y-%m-%d")
        if enddate != datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d"):
            return True
    return False

def get_exceptions():
    return ("Supervisor not assigned sincemultiple users found with same EMP id" if len(rail.result('search_for_user_with_empid') or []) > 1 else '') + (
        rail.result('log_supervisor_disabled') if rail.result('log_supervisor_disabled') else '') + (
            rail.result('log_foreign_supervisor_not_received') if rail.result('log_foreign_supervisor_not_received') else '')


def get_supervisor_status_escalation():
    # Recipe #13/#34: only multiple-match and disabled-supervisor escalate the
    # entry status to Exception; created/not-received keep the original status.
    return ("Supervisor not assigned sincemultiple users found with same EMP id" if len(rail.result('search_for_user_with_empid') or []) > 1 else '') + (
        rail.result('log_supervisor_disabled') or '')

def get_udf_values_from_userdetails():
    user_customfield = rail.result('get_user_data_14')[
        0]['userDetails']['customFieldValues']

    def _get_lower(display_text):
        # Mismatch tests compare lowercase conf values; recipe downcases both sides
        return (rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', display_text, 'text', '') or '').lower()

    return {
        'dob': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Date of Birth', 'text', ''),
        'title': _get_lower('Title'),
        'worker_subType': _get_lower('Worker Sub Type'),
        'yearsofservice': _get_lower('Years of Service'),
        'hrm': _get_lower('HRM'),
        'cont_yearsofservice': _get_lower('Continuous Years of Service - YOS'),
        'timeoffservicedate': _get_lower('Time off Service Date - YOSS'),
        'gender': _get_lower('Gender'),
        'function': _get_lower('Function'),
        'work_shift': _get_lower('Work Shift')
    }

def get_startday_of_nexttimesheet():
    # Recipe #157/#158: timesheet period end + 1 day, falling back to today
    # when the user has no timesheet (details task skipped).
    details = rail.result('get_timesheet_details')
    end_date = ((details or {}).get('dateRange') or {}).get('endDate') or {}
    if 'day' in end_date:
        return (datetime.strptime(
            str(end_date['year']) + '-' + str(end_date['month']) + '-' + str(end_date['day']),
            "%Y-%m-%d") + timedelta(days=1)).date().strftime("%Y-%m-%d")
    return datetime.now().date().strftime("%Y-%m-%d")


def get_current_group_display_text(group_key, item_key):
    """First entry's displayText from GetEffectiveUserGroupMembership, '' when absent."""
    membership = rail.result('get_effectiveusergroupmembership') or {}
    groups = membership.get(group_key) or []
    if not groups:
        return ''
    inner = ((groups[0] or {}).get(item_key) or {}).get(item_key) or {}
    return inner.get('displayText') or ''


def compare_dates_to_today(dag_run):
    exemptioneff_date = False
    workshiftchange_effectivedate = False
    effectivedateof_workertype = False
    cflrvlocationchange_effectivedate = False

    if dag_run.conf['exemption_eff_date']:
        cf_lrv_job_exempt_eff_date = datetime.strptime(
            dag_run.conf['exemption_eff_date'], "%Y-%m-%d")
        if cf_lrv_job_exempt_eff_date.date() == datetime.now().date():
            exemptioneff_date = True

    if dag_run.conf['work_shift_change_effective_date']:
        work_shift_change_effective_date = datetime.strptime(
            dag_run.conf['work_shift_change_effective_date'], "%Y-%m-%d")
        if work_shift_change_effective_date.date() == datetime.now().date():
            workshiftchange_effectivedate = True

    if dag_run.conf['effective_date_of_worker_type']:
        effective_date_of_worker_type = datetime.strptime(
            dag_run.conf['effective_date_of_worker_type'], "%Y-%m-%d")
        if effective_date_of_worker_type.date() == datetime.now().date():
            effectivedateof_workertype = True

    if dag_run.conf['CF_LRV_Location_Change_Effective_Date']:
        location_change_eff_date = datetime.strptime(
            dag_run.conf['CF_LRV_Location_Change_Effective_Date'], "%Y-%m-%d")
        if location_change_eff_date.date() == datetime.now().date():
            cflrvlocationchange_effectivedate = True

    return {
        'exemption_eff_date': exemptioneff_date,
        'work_shift_change_effective_date': workshiftchange_effectivedate,
        'effective_date_of_workertype': effectivedateof_workertype,
        'cf_lrv_location_change_effective_date': cflrvlocationchange_effectivedate
    }

def build_costcenter_list():
    res = rail.result('get_costcenter_group_data')
    rows = res.get('rows',[])
    out = []
    for r in rows:
        name = r['cells'][0]['textValue']
        uri = r['cells'][0]['uri']
        cells = r['cells'][1]['cellCollection']
        fullpath = " / ".join(c.get('textValue', '') for c in cells)
        out.append({'name': name, 'uri': uri, 'fullpath': fullpath})
    return out

def build_legalentity_list():
    res = rail.result('get_division_group_data')
    rows = res.get('rows',[])
    out = []
    for r in rows:
        name = r['cells'][0]['textValue']
        uri = r['cells'][0]['uri']
        cells = r['cells'][1]['cellCollection']
        fullpath = " / ".join(c.get('textValue', '') for c in cells)
        out.append({'name': name, 'uri': uri, 'fullpath': fullpath})
    return out


def get_status_and_details_for_update(dag_run):
    # Recipe #313-#316: status = Exception when any error message was logged;
    # details = error messages + field-change entries, or error messages +
    # "No field changes were received" when nothing changed.
    has_log_entries = ','.join(list(
        map(lambda v: v['properties']['value'], rail.load_all_records(rail.result('log_entries')))))
    has_exception_message = ','.join(list(map(
        lambda v: v['properties']['value'], rail.load_all_records(rail.result('exception_log')))))
    message = "Exception" if has_exception_message else "Success"
    if has_log_entries:
        details = (has_exception_message + ',' + has_log_entries) if has_exception_message else has_log_entries
    else:
        details = (has_exception_message + ',' if has_exception_message else '') + "No field changes were received"
    return {
        "jobid": dag_run.conf['parentjobid'],
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Update",
        "status": message,
        'details': details,
        "childjobid": get_dagrun_ecid(dag_run),
    }


def build_timeoff_uri_list(enabled_timeoff_types, requested_types):
    if not enabled_timeoff_types or not requested_types:
        return []
    
    enabled_list = enabled_timeoff_types.get('response', {}).get('d', []) if isinstance(enabled_timeoff_types, dict) else enabled_timeoff_types
    
    matched = []
    for requested_name in requested_types:
        for enabled_type in enabled_list:
            name = enabled_type.get('displayText') or enabled_type.get('name')
            if name == requested_name.strip():
                matched.append({
                    'uri': enabled_type.get('uri'),
                    'name': name
                })
                break
    
    return matched


def flatten_assigned_timeoff_types(assigned_timeoff_types):
    """
    BulkGetTimeOffTypeAssignmentsForUsers returns one item per user, each holding
    timeOffTypeAssignmentsDetails.timeOffTypes (the actual assigned types) —
    same nesting the recipe walks with its double foreach (steps 20-22).
    """
    assigned_list = assigned_timeoff_types.get('response', {}).get('d', []) if isinstance(assigned_timeoff_types, dict) else assigned_timeoff_types
    return [
        entry
        for item in (assigned_list or [])
        for entry in (((item or {}).get('timeOffTypeAssignmentsDetails') or {}).get('timeOffTypes') or [])
    ]


def build_update_matched_from_workato(enabled_timeoff_types, requested_types_string, assigned_timeoff_types):
    """Build matched timeoff type list exactly as Workato (steps 11-20)."""
    if not requested_types_string:
        return []

    requested_names = [n.strip() for n in requested_types_string.split('|') if n.strip()]

    enabled_list = enabled_timeoff_types.get('response', {}).get('d', []) if isinstance(enabled_timeoff_types, dict) else enabled_timeoff_types
    assigned_types = flatten_assigned_timeoff_types(assigned_timeoff_types)

    assigned_names = set([a.get('displayText') or a.get('name') for a in assigned_types if a])

    matched = []
    for requested_name in requested_names:
        for enabled_type in enabled_list:
            name = enabled_type.get('displayText') or enabled_type.get('name')
            if name == requested_name:
                action = 'update' if name in assigned_names else 'add'
                matched.append({
                    'uri': enabled_type.get('uri'),
                    'name': name,
                    'action': action
                })
                break

    return matched


def build_update_timeoff_lists(enabled_timeoff_types, requested_types_string, assigned_timeoff_types):
    """Build payload for update timeoff logic including payout list."""
    if not requested_types_string:
        requested_names = []
    else:
        requested_names = [n.strip() for n in requested_types_string.split('|') if n.strip()]

    enabled_list = enabled_timeoff_types.get('response', {}).get('d', []) if isinstance(enabled_timeoff_types, dict) else enabled_timeoff_types
    assigned_types = flatten_assigned_timeoff_types(assigned_timeoff_types)

    assigned_names = set([a.get('displayText') or a.get('name') for a in assigned_types if a])
    assigned_map = {a.get('displayText') or a.get('name'): a for a in assigned_types if a}

    matched = build_update_matched_from_workato(enabled_timeoff_types, requested_types_string, assigned_timeoff_types)

    # Recipe steps 23-25: previously assigned types that are NOT in the new
    # requested set get a payout call before being unassigned.
    to_remove = []
    for assigned_item in assigned_types:
        name = assigned_item.get('displayText') or assigned_item.get('name')
        if name not in requested_names:
            to_remove.append({'uri': assigned_item.get('uri'), 'name': name})

    final_uris = [item.get('uri') for item in matched if item.get('uri')]
    newly_added = [item for item in matched if item.get('action') == 'add']
    previously_assigned = [item for item in matched if item.get('action') == 'update']

    return {
        'matched': matched,
        'to_remove': to_remove,
        'final_uris': final_uris,
        'new': newly_added,
        'previously_assigned': previously_assigned
    }


def handle_termination_for_user(useruri, termination_date, assigned_timeoff_types):
    """Placeholder that gets called in termination event path."""
    # Workato-specific termination handling may delete or deactivate timeoff assignments.
    # We keep a minimal stub; specialized implementation may be added as required.
    return {
        'useruri': useruri,
        'termination_date': termination_date,
        'assigned': assigned_timeoff_types,
        'status': 'terminated'
    }


def convert_policy_set_with_script_target(policy_response):
    if not policy_response:
        return []
    
    policy_list = policy_response if isinstance(policy_response, list) else [policy_response]
    
    converted_list = json.loads(
        json.dumps(policy_list).replace(
            'null', '"effective"'
        ).replace(
            '"script"', '"scriptTarget"'
        )
    )
    
    return converted_list


def validate_policy_structure(policy_set, timeoff_type):
    """
    Validate policy structure and return as-is (following Workato approach).
    Handles KeyError gracefully when policy structures are missing.
    """
    if not policy_set:
        raise ValueError(f"No default policy set available for time off type '{timeoff_type}'. Please ensure default accrual policies are assigned in Replicon UI.")
    
    # Check for required structure
    if 'timeOffBalanceEventScripts' not in policy_set:
        raise KeyError(f"timeOffBalanceEventScripts not found in policy set for time off type '{timeoff_type}'. Default accrual policies are not properly configured.")
    
    # Log the policy structure for debugging
    scripts = policy_set.get('timeOffBalanceEventScripts', [])
    script_names = [script.get('script', {}).get('name', 'Unknown') for script in scripts]
    return policy_set



def build_shift_worker_policy_with_offset_check(shift_worker_policy_response, dag_run=None):
    if not shift_worker_policy_response or not dag_run:
        return []
    
    response_items = shift_worker_policy_response if isinstance(shift_worker_policy_response, list) else [shift_worker_policy_response]
    
    workshift_effdate_str = dag_run.conf['work_shift_change_effective_date']
    startdate_str = dag_run.conf['startdate']
    
    policy_entries = []
    
    for item in response_items:
        offset_value = item.get('startOffset', {}).get('offsetValue')
        
        if offset_value is None or offset_value not in [0, 1]:
            continue
        
        policy_set = item.get('policySet')
        
        if offset_value == 0:
            # Use current effective date (workshift_effdate if present, else startdate)
            date_to_use = workshift_effdate_str if workshift_effdate_str else startdate_str
            
            if date_to_use:
                # Parse the date
                effective_date = datetime.strptime(date_to_use, "%Y-%m-%d")
                description = f"Effective on {effective_date.strftime('%m/%d/%Y')}"
                
                policy_entry = {
                    "effectiveDate": {
                        "day": effective_date.day,
                        "month": effective_date.month,
                        "year": effective_date.year
                    },
                    "description": description,
                    "policySet": policy_set
                }
            else:
                continue  # Skip if no date available
                
        elif offset_value == 1:
            # 01/01 of (today + 12 months) when workshift eff date present, else (startdate + 12 months)
            if workshift_effdate_str:
                next_year = (datetime.now() + relativedelta(months=12)).year
            elif startdate_str:
                next_year = (datetime.strptime(startdate_str, "%Y-%m-%d") + relativedelta(months=12)).year
            else:
                continue  # Skip if no date available

            policy_entry = {
                "effectiveDate": {
                    "day": 1,
                    "month": 1,
                    "year": next_year
                },
                "description": f"Effective on 01/01/{next_year}",
                "policySet": policy_set
            }
        
        converted_entry = json.loads(
            json.dumps(policy_entry).replace(
                'null', '"effective"'
            ).replace(
                '"script"', '"scriptTarget"'
            )
        )
        
        policy_entries.append(converted_entry)
    
    return policy_entries


def get_timeoff_accrual_hours(employment_month):
    """
    Calculate yearly accrual hours based on employment month.
    
    Implements Workato Step 5: Calculate yearly accrual based on employment month.
    
    Args:
        employment_month: Integer month (1-12) when employee started
    
    Returns:
        Integer hours: 12 (< month 4), 10 (month 4-6), 6 (month 7-9), 3 (month 10-12)
    """
    if not employment_month:
        return 0
    
    month = int(employment_month)
    if month < 4:
        return 12
    elif 4 <= month <= 6:
        return 10
    elif 7 <= month <= 9:
        return 6
    elif 10 <= month <= 12:
        return 3
    else:
        return 0
    

def get_startdate_month(employment_month, is_january_first):

    if is_january_first:
        return "JANUARY 1ST"

    month = int(employment_month)
    if 1 <= month <= 3:
        return "JANUARY 2ND TILL MARCH 31ST"
    if 4 <= month <= 6:
        return "APRIL 1ST TILL JUNE 30"
    if 7 <= month <= 9:
        return "JULY 1ST TILL SEPTEMBER 30"
    if 10 <= month <= 12:
        return "OCTOBER 1ST TILL DECEMBER 31ST"
    return None
        

def build_timeoff_policy_with_offset_check(policy_response, dag_run=None):
    if not policy_response or not dag_run:
        return []
    
    response_items = policy_response if isinstance(policy_response, list) else [policy_response]
    
    startdate_str = dag_run.conf['startdate']
    
    policy_entries = []
    
    for item in response_items:
        offset_value = item.get('startOffset', {}).get('offsetValue')
        policy_set = item.get('policySet')

        if offset_value is None:
            continue  # Skip entries without a start offset

        if offset_value == 0:

            if startdate_str:
                effective_date = datetime.strptime(startdate_str, "%Y-%m-%d")
                description = f"Effective on {effective_date.strftime('%m/%d/%Y')}"
                
                policy_entry = {
                    "effectiveDate": {
                        "day": effective_date.day,
                        "month": effective_date.month,
                        "year": effective_date.year
                    },
                    "description": description,
                    "policySet": policy_set
                }
            else:
                continue  # Skip if no date available
                
        else:
            # Use 01/01 of (startdate + offsetValue*12 months)
            if startdate_str:
                effective_date = datetime.strptime(startdate_str, "%Y-%m-%d")
                # Add offsetValue * 12 months using relativedelta
                months_to_add = int(offset_value) * 12
                next_year_jan_first = (effective_date + relativedelta(months=months_to_add)).replace(month=1, day=1)
                
                year_str = next_year_jan_first.strftime("%Y")
                description = f"Effective on 01/01/{year_str}"
                
                policy_entry = {
                    "effectiveDate": {
                        "day": 1,
                        "month": 1,
                        "year": next_year_jan_first.year
                    },
                    "description": description,
                    "policySet": policy_set
                }
            else:
                continue  # Skip if no date available
    
        converted_entry = json.loads(
            json.dumps(policy_entry).replace(
                'null', '"effective"'
            ).replace(
                '"script"', '"scriptTarget"'
            )
        )
        
        policy_entries.append(converted_entry)
    
    return policy_entries

def build_rehire_timeoff_policy_with_offset_check(policy_response, dag_run):
    """
    Build rehire month timeoff policy with offset checking for fixed-term rehire employees.
    
    Uses yoss (rehire/service start date) instead of hire date.
    
    Implements different logic based on offsetValue:
    - offsetValue == 0: Use yoss date directly
    - offsetValue > 0: Use 01/01 of (yoss + offsetValue*12 months)
    
    Args:
        policy_response: Policy response from API
        dag_run: Airflow DagRun for accessing conf variables (yoss, startdate)
    
    Returns:
        List of policySetScheduleEntries with proper formatting
    """
    if not policy_response or not dag_run:
        return []
    
    response_items = policy_response if isinstance(policy_response, list) else [policy_response]
    
    yoss_str = dag_run.conf['yoss']
    if not yoss_str:
        return []

    rehire_date = datetime.strptime(yoss_str, "%Y-%m-%d")
    rehire_month_name = rehire_date.strftime('%B').lower()

    policy_entries = []

    for item in response_items:
        offset_value = item.get('startOffset', {}).get('offsetValue')
        policy_set = item.get('policySet')

        if offset_value is None:
            continue  # Skip entries without a start offset

        if offset_value == 0:
            policy_entry = {
                "effectiveDate": {
                    "day": rehire_date.day,
                    "month": rehire_date.month,
                    "year": rehire_date.year
                },
                "description": f"Effective on {rehire_date.strftime('%m/%d/%Y')}",
                "policySet": policy_set
            }
        elif offset_value > 0:
            # Effective on the rehire month/day of (yoss + offsetValue*12 months); description says 01/01
            target_year = (rehire_date + relativedelta(months=int(offset_value) * 12)).year
            policy_entry = {
                "effectiveDate": {
                    "day": rehire_date.day,
                    "month": rehire_date.month,
                    "year": target_year
                },
                "description": f"Effective on 01/01/{target_year}",
                "policySet": policy_set
            }
        else:
            continue  # Skip invalid offset values

        converted_entry = json.loads(
            json.dumps(policy_entry).replace(
                'null', '"effective"'
            ).replace(
                '"script"', '"scriptTarget"'
            ).replace(
                # rehire recipe uses gsub("january", ...) (unquoted
                # substring), so it replaces the month inside "urn:replicon:month:january".
                # A quoted '"january"' never matches the URI and leaves accrual at January.
                'january', rehire_month_name
            )
        )

        policy_entries.append(converted_entry)

    return policy_entries

def _ordinal(n):
    if 11 <= (n % 100) <= 13:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return f'{n}{suffix}'

def build_parttime_timeoff_policy_with_date_replacement(policy_response, dag_run=None):
    if not policy_response or not dag_run:
        return []
    
    startdate_str = dag_run.conf.get('startdate')
    
    if not startdate_str:
        return []
    
    # Parse startdate
    effective_date = datetime.strptime(startdate_str, "%Y-%m-%d")
    
    # Add 6 months using relativedelta (handles edge cases automatically)
    future_date_precise = effective_date + relativedelta(months=6)

    split_future_date = split_date_string(future_date_precise.strftime("%Y-%m-%d"), split_type='int')

    day_value = split_future_date['day']
    day_ordinal = _ordinal(day_value)
    
    # Extract month in long format (lowercase)
    month_name = future_date_precise.strftime('%B').lower()
    
    # Convert policy response to list
    policy_list = policy_response if isinstance(policy_response, list) else [policy_response]
    
    # Convert to JSON and apply string replacements
    policy_json_str = json.dumps(policy_list)
    
    # Replace null with "effective"
    policy_json_str = policy_json_str.replace('null', '"effective"')
    
    # Replace "script" with "scriptTarget"
    policy_json_str = policy_json_str.replace('"script"', '"scriptTarget"')
    
    # Replace "1st" placeholder with derived day ordinal.
    # Workato uses gsub("1st", ...) (unquoted substring), so the
    # placeholder is replaced inside the day-of-month URI
    # "urn:replicon:monthly-frequency-start-day-option:1st". A quoted replace of
    # '"1st"' never matches the URI-embedded value and leaves accrual/reset at the 1st.
    policy_json_str = policy_json_str.replace('1st', day_ordinal)

    # Replace "january" placeholder with derived month.
    # Workato uses gsub("january", ...) (unquoted substring), so it
    # also replaces the month inside "urn:replicon:month:january".
    policy_json_str = policy_json_str.replace('january', month_name)
    
    # Convert back to list
    converted_policy = json.loads(policy_json_str)
    
    return converted_policy if isinstance(converted_policy, list) else [converted_policy]


def calculate_yoss_years_and_month(yoss_str, timezone='Asia/Tokyo'):
    """
    Calculate continuous years of service (YOSS) from yoss date with timezone awareness.

    Args:
        yoss_str: Date string in YYYY-MM-DD format
        timezone: Timezone for calculations (default: Asia/Tokyo for Japan operations)

    Returns:
        dict with:
        - yoss_years: Float years of continuous service (rounded 2 decimals)
        - service_month_uri: "urn:replicon:month:{month_lowercase}"
        - yoss_date: Parsed datetime object
    """
    if not yoss_str:
        return {
            'yoss_years': 0,
            'service_month_uri': 'urn:replicon:month:january',
            'yoss_date': None
        }

    try:
        # Parse date and add timezone awareness
        from zoneinfo import ZoneInfo
        yoss_date = datetime.strptime(yoss_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo(timezone))
        today = datetime.now(ZoneInfo(timezone))

        # Recipe: ((today - yoss).to_f / 365).round(2)
        days_diff = (today - yoss_date).days
        yoss_years = round(days_diff / 365, 2)

        # Extract month for lookup
        month_name = yoss_date.strftime('%B').lower()
        service_month_uri = f"urn:replicon:month:{month_name}"

        return {
            'yoss_years': yoss_years,
            'service_month_uri': service_month_uri,
            'yoss_date': yoss_date
        }
    except (ValueError, TypeError) as e:
        # Fallback for invalid dates
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Invalid YOSS date format: {yoss_str}, error: {e}")
        return {
            'yoss_years': 0,
            'service_month_uri': 'urn:replicon:month:january',
            'yoss_date': None
        }


def determine_service_band(yoss_years):
    """
    Determine service band based on years of continuous service.
    
    Service bands:
    - 0.5: Less than 0.49 years (< 6 months)
    - 1.5: 0.49 to 1.49 years (6 months to <18 months)
    - 2.5: 1.49 to 2.49 years (18 months to <30 months)
    - 3.5: 2.49 to 3.49 years (30 months to <42 months)
    - 4.5: 3.49 to 4.49 years (42 months to <54 months)
    - 5.5: 4.49 to 5.49 years (54 months to <66 months)
    - 6.5: 5.49+ years (66+ months)
    
    Args:
        yoss_years: Float years of service
    
    Returns:
        str: Service band value (0.5, 1.5, 2.5, etc.)
    """
    if not isinstance(yoss_years, (int, float)):
        yoss_years = float(yoss_years) if yoss_years else 0

    # Recipe steps 7-20: sequential overlapping conditions, last match wins;
    # no match (<= 0.49 years) leaves the variable at its initial "0".
    band = "0"
    if 0.49 < yoss_years < 1.5:
        band = "0.5"
    if 1.49 < yoss_years < 2.5:
        band = "1.5"
    if 2.49 < yoss_years < 3.5:
        band = "2.5"
    if 3.49 < yoss_years < 4.5:
        band = "3.5"
    if 4.49 < yoss_years < 5.5:
        band = "4.5"
    if 5.49 < yoss_years < 6.5:
        band = "5.5"
    if yoss_years > 6.49:
        band = "6.5"
    return band


def lookup_accrual_from_mapper(mapper_entries, yoss_years):
    """
    Extract accrual values from mapper based on service year bands.
    
    Mapper structure:
    {
        'timeofftype': '01. JPN_年次有給休暇 Annual Paid Leave (Regular)',
        'workersubtype': 'Regular',
        'yos': '1|4|7|10',           # Year of service band thresholds
        'accrual': '12|10|6|3'       # Corresponding accrual values
    }
    
    Logic:
    - yos = "1|4|7|10" means bands: <1yr, 1-<4yr, 4-<7yr, 7+yr
    - accrual = "12|10|6|3" means: 12 days, 10 days, 6 days, 3 days
    - yoss_years = 2.5 falls into second band (1-<4) = 10 days
    
    Args:
        mapper_entries: List of mapper dicts matching the timeofftype
        yoss_years: Float years of continuous service
    
    Returns:
        dict with accrual_values as pipe-separated string (e.g., '10' or '10|20|30')
    """
    if not mapper_entries or len(mapper_entries) == 0:
        return {'accrual_values': '0'}
    
    try:
        # Get first matching entry
        mapper_entry = mapper_entries[0]
        
        # Extract yos bands and accrual values
        yos_bands_str = mapper_entry.get('yos', '')
        accrual_str = mapper_entry.get('accrual', '')
        
        if not yos_bands_str or not accrual_str:
            return {'accrual_values': '0'}
        
        # Parse bands and accruals
        yos_bands = [float(x.strip()) for x in yos_bands_str.split('|')]
        accrual_values = [x.strip() for x in accrual_str.split('|')]
        
        # Find which band yoss_years falls into
        selected_accrual = accrual_values[-1]  # Default to last (highest band)
        
        for idx, band_threshold in enumerate(yos_bands):
            if yoss_years < band_threshold:
                selected_accrual = accrual_values[idx]
                break
        
        return {
            'accrual_values': accrual_str,  # Return full pipe-separated string for indexing
            'selected_accrual': selected_accrual,  # Keep individual selected value
            'yoss_years': yoss_years,
            'yos_bands': yos_bands_str,
            'all_accruals': accrual_str,
            'selected_band': selected_accrual
        }
    except Exception:
        return {'accrual_values': '0'}


def build_policy_item_beginning_of_year(service_band, accrual_values_str):
    """Step 24: Policy for beginning of year"""
    # value: direct mapping (0.5→[0], 1.5→[1], etc.)
    # existingvalue: offset mapping (0.5→[0], 1.5→[0], 2.5→[1], etc.)
    accrual_values = [x.strip() for x in accrual_values_str.split('|')]
    band_to_value_idx = {'0.5': 0, '1.5': 1, '2.5': 2, '3.5': 3, '4.5': 4, '5.5': 5, '6.5': 6}
    band_to_existing_idx = {'0.5': 0, '1.5': 0, '2.5': 1, '3.5': 2, '4.5': 3, '5.5': 4, '6.5': 5}
    
    value_idx = min(band_to_value_idx.get(service_band, 6), len(accrual_values) - 1)
    existing_idx = min(band_to_existing_idx.get(service_band, 6), len(accrual_values) - 1)
    
    today_beg_of_year = datetime.now().replace(month=1, day=1).date()
    
    return {
        'yoss': service_band,
        'numberofpolicy': 1,
        'effectivedate': {'day': today_beg_of_year.day, 'month': today_beg_of_year.month, 'year': today_beg_of_year.year},
        'value': accrual_values[value_idx],
        'existingvalue': accrual_values[existing_idx]
    }

def add_6_months_to_date(startdate_str):
    """
    Add 6 months to a date string.
    Handles edge cases like Jan 31 + 6 months = Jul 31, Feb 29, etc.
    
    Args:
        startdate_str: Date string in YYYY-MM-DD format
    
    Returns:
        str: Updated date in YYYY-MM-DD format
    """
    startdate = datetime.strptime(startdate_str, '%Y-%m-%d')
    # Add 6 months using relativedelta (handles edge cases automatically)
    effective_date = startdate + relativedelta(months=6)
    return effective_date.strftime('%Y-%m-%d')

def build_policy_item_startdate_plus_6months(service_band, accrual_values_str, startdate_str):
    """Step 27: Policy for startdate + 6 months (for yoss < 0.5)"""
    accrual_values = [x.strip() for x in accrual_values_str.split('|')]

    # Recipe uses `<= "0.5"` for the first rung, so band "0" also selects index 0
    band_to_value_idx = {'0': 0, '0.5': 0, '1.5': 1, '2.5': 2, '3.5': 3, '4.5': 4, '5.5': 5, '6.5': 6}
    band_to_existing_idx = {'0': 0, '0.5': 0, '1.5': 0, '2.5': 1, '3.5': 2, '4.5': 3, '5.5': 4, '6.5': 5}
    
    value_idx = min(band_to_value_idx.get(service_band, 6), len(accrual_values) - 1)
    existing_idx = min(band_to_existing_idx.get(service_band, 6), len(accrual_values) - 1)
    
    # Use helper function for date calculations
    future_date_str = add_6_months_to_date(startdate_str)
    effective_date = datetime.strptime(future_date_str, '%Y-%m-%d')
    
    return {
        'yoss': service_band,
        'numberofpolicy': 1,
        'effectivedate': {'day': effective_date.day, 'month': effective_date.month, 'year': effective_date.year},
        'value': accrual_values[value_idx],
        'existingvalue': accrual_values[existing_idx]
    }


def build_policy_item_startdate(service_band, accrual_values_str, startdate_str):
    """Step 30: Policy for startdate (for yoss >= 0.5)"""
    # value: direct mapping
    # existingvalue: offset mapping
    accrual_values = [x.strip() for x in accrual_values_str.split('|')]
    band_to_value_idx = {'0.5': 0, '1.5': 1, '2.5': 2, '3.5': 3, '4.5': 4, '5.5': 5, '6.5': 6}
    band_to_existing_idx = {'0.5': 0, '1.5': 0, '2.5': 1, '3.5': 2, '4.5': 3, '5.5': 4, '6.5': 5}
    
    value_idx = min(band_to_value_idx.get(service_band, 6), len(accrual_values) - 1)
    existing_idx = min(band_to_existing_idx.get(service_band, 6), len(accrual_values) - 1)
    
    startdate = datetime.strptime(startdate_str, '%Y-%m-%d').date()
    
    return {
        'yoss': service_band,
        'numberofpolicy': 1,
        'effectivedate': {'day': startdate.day, 'month': startdate.month, 'year': startdate.year},
        'value': accrual_values[value_idx],
        'existingvalue': accrual_values[existing_idx]
    }


def build_standard_parttime_policies_multiple_years(service_band, accrual_values_str, yoss_str, current_effectivedates_list):
    """
    Recipe steps 32-37 (yoss band "0"): six annual entries based on the YOSS date,
    (yoss + 6 months + 12*k months).beginning_of_year with value [k], existing [k-1].
    """
    accrual_values = accrual_values_str.split('|')

    # Start with existing list
    policies = list(current_effectivedates_list) if current_effectivedates_list else []

    yoss_date = datetime.strptime(yoss_str, '%Y-%m-%d')

    for year_offset in range(1, 7):
        total_months = 6 + (12 * year_offset)
        effective_date = (yoss_date + relativedelta(months=total_months)).replace(month=1, day=1)

        value_idx = year_offset
        existing_idx = year_offset - 1
        value = accrual_values[value_idx] if value_idx < len(accrual_values) else accrual_values[-1]
        existing_value = accrual_values[existing_idx] if existing_idx < len(accrual_values) else accrual_values[-1]

        policies.append({
            'yoss': service_band,
            'effectivedate': {'day': effective_date.day, 'month': effective_date.month, 'year': effective_date.year},
            'value': value,
            'existingvalue': existing_value
        })

    return policies


def build_standard_parttime_policies_from_yoss_date(service_band, accrual_values_str, yoss_str, current_effectivedates_list):
    """
    Build multiple annual policies for years 1-6 using YOSS date.
    Workato steps 40-46: IF startdate > beginning_of_year
    Uses yoss date as base: yoss + 12, 24, 36, 48, 60, 72 months
    """
    accrual_values = accrual_values_str.split('|')
    policies = list(current_effectivedates_list) if current_effectivedates_list else []

    yoss_date = datetime.strptime(yoss_str, '%Y-%m-%d')

    # Recipe steps 40-46: SEVEN entries (yoss + 12..84 months).beginning_of_year;
    # value = [k-1]; existing = [1] for the first entry (recipe quirk), then [k-2].
    for year_offset in range(1, 8):
        total_months = 12 * year_offset
        effective_date = (yoss_date + relativedelta(months=total_months)).replace(month=1, day=1)

        value_idx = year_offset - 1
        existing_idx = 1 if year_offset == 1 else year_offset - 2
        value = accrual_values[value_idx] if value_idx < len(accrual_values) else accrual_values[-1]
        existing_value = accrual_values[existing_idx] if existing_idx < len(accrual_values) else accrual_values[-1]

        policies.append({
            'yoss': service_band,
            'effectivedate': {'day': effective_date.day, 'month': effective_date.month, 'year': effective_date.year},
            'value': value,
            'existingvalue': existing_value
        })

    return policies


def build_standard_parttime_policies_from_startdate_years(service_band, accrual_values_str, startdate_str, current_effectivedates_list):
    """
    Build multiple annual policies for years 1-6 using STARTDATE.
    Workato steps 48-53: ELSE branch (startdate <= beginning_of_year)
    Uses startdate as base: startdate + 12, 24, 36, 48, 60, 72 months
    """
    accrual_values = accrual_values_str.split('|')
    policies = list(current_effectivedates_list) if current_effectivedates_list else []

    startdate = datetime.strptime(startdate_str, '%Y-%m-%d')

    # Recipe steps 48-53: six entries (startdate + 12..72 months).beginning_of_year;
    # value = [k], existing = [k-1].
    for year_offset in range(1, 7):
        total_months = 12 * year_offset
        effective_date = (startdate + relativedelta(months=total_months)).replace(month=1, day=1)

        value_idx = year_offset
        existing_idx = year_offset - 1
        value = accrual_values[value_idx] if value_idx < len(accrual_values) else accrual_values[-1]
        existing_value = accrual_values[existing_idx] if existing_idx < len(accrual_values) else accrual_values[-1]

        policies.append({
            'yoss': service_band,
            'effectivedate': {'day': effective_date.day, 'month': effective_date.month, 'year': effective_date.year},
            'value': value,
            'existingvalue': existing_value
        })

    return policies

def build_standard_parttime_policies_by_service_band(service_band, accrual_values_str, yoss_str, current_effectivedates_list):
    """
    Generic function for all service bands 1.5-6.5 (Workato steps 54-78)
    Automatically calculates month offsets and value indices based on service band
    """
    accrual_values = accrual_values_str.split('|')
    policies = list(current_effectivedates_list) if current_effectivedates_list else []

    band = float(service_band)
    # Recipe steps 54-72 only cover bands 1.5-5.5 (6.5 adds nothing);
    # bands "0"/"0.5" are handled by the earlier branches and must not add entries here.
    if band < 1.5:
        return policies

    band_ceil = int(band) if band == int(band) else int(band) + 1  # ceil
    
    start_month = band_ceil * 12
    num_policies = 7 - band_ceil
    value_start_index = band_ceil
    existing_start_index = band_ceil - 1
    
    for i in range(num_policies):
        total_months = start_month + (12 * i)
        
        yoss_date = datetime.strptime(yoss_str, '%Y-%m-%d')
        # Add months using relativedelta and set to January 1st
        effective_date = (yoss_date + relativedelta(months=total_months)).replace(month=1, day=1)
        
        value_idx = value_start_index + i
        existing_idx = existing_start_index + i
        
        value = accrual_values[value_idx] if value_idx < len(accrual_values) else accrual_values[-1]
        existing_value = accrual_values[existing_idx] if existing_idx < len(accrual_values) else accrual_values[-1]
        
        policies.append({
            'yoss': service_band,
            'effectivedate': {'day': effective_date.day, 'month': effective_date.month, 'year': effective_date.year},
            'value': value,
            'existingvalue': existing_value
        })
    
    return policies


def get_unique_effective_dates(effectivedates_list):
    if not effectivedates_list:
        return []
    
    # Use dict to preserve order while removing duplicates (Python 3.7+)
    seen_dates = {}
    for item in effectivedates_list:
        if isinstance(item, dict) and 'effectivedate' in item:
            effective_date = item['effectivedate']
            # Convert to string for comparison if it's a dict
            if isinstance(effective_date, dict):
                date_str = f"{effective_date.get('year')}-{effective_date.get('month'):02d}-{effective_date.get('day'):02d}"
            else:
                date_str = str(effective_date)
            seen_dates[date_str] = effective_date
    
    return list(seen_dates.values())


def check_policy_schedule_exists(policy_schedules):
    """
    Check if policy schedules exist from API response
    Equivalent to Workato Step 80: if present effectiveDate.day
    
    Args:
        policy_schedules: Response from GetDefaultTimeOffTypePolicyScheduleForUser API
        
    Returns:
        True if policy exists with effectiveDate, False otherwise
    """
    if not policy_schedules or not isinstance(policy_schedules, list):
        return False
    
    if len(policy_schedules) == 0:
        return False
    
    first_schedule = policy_schedules[0]
    if not isinstance(first_schedule, dict):
        return False
    
    effective_date = first_schedule.get('effectiveDate', {})
    return effective_date and effective_date.get('day') is not None


def extract_accrual_balance_setup(policy_schedules):
    """
    Extract accrual balance setup from policy schedules
    Equivalent to Workato Step 81-89: Extract "Yearly Accrual" script parameters
    
    Args:
        policy_schedules: Response from GetDefaultTimeOffTypePolicyScheduleForUser API
        
    Returns:
        Dict with accrual parameters: accrual_amount, accrue_on_month, accrue_on_day, precedence
    """
    if not policy_schedules or len(policy_schedules) == 0:
        return {}
    
    policy_set = policy_schedules[0].get('policySet', {})
    scripts = policy_set.get('timeOffBalanceEventScripts', [])
    
    # Find "Yearly Accrual" script
    accrual_script = next(
        (s for s in scripts if s.get('script', {}).get('name') == 'Yearly Accrual'),
        None
    )
    
    if not accrual_script:
        return {}
    
    # Extract parameters
    params = {}
    for param in accrual_script.get('additionalParameters', []):
        key_uri = param.get('keyUri', '')
        value = param.get('value', {})
        
        if 'accrual-annual-amount' in key_uri:
            params['accrual_amount'] = value.get('number')
        elif 'accrue-on-month' in key_uri:
            params['accrue_on_month'] = value.get('uri', '')
        elif 'accrue-on-day-of-month' in key_uri:
            params['accrue_on_day'] = value.get('uri', '')
        elif 'precedence' in key_uri:
            params['precedence'] = value.get('number')
    
    return params


def extract_yearly_reset_setup(policy_schedules):
    """
    Extract yearly reset setup from policy schedules
    Equivalent to Workato Step 83-88: Extract "Yearly Reset" script parameters
    
    Args:
        policy_schedules: Response from GetDefaultTimeOffTypePolicyScheduleForUser API
        
    Returns:
        Dict with reset parameters: reset_balance_amount, reset_on_month, reset_on_day, precedence
    """
    if not policy_schedules or len(policy_schedules) == 0:
        return {}
    
    policy_set = policy_schedules[0].get('policySet', {})
    scripts = policy_set.get('timeOffBalanceEventScripts', [])
    
    # Find "Yearly Reset" script
    reset_script = next(
        (s for s in scripts if s.get('script', {}).get('name') == 'Yearly Reset'),
        None
    )
    
    if not reset_script:
        return {}
    
    # Extract parameters
    params = {}
    for param in reset_script.get('additionalParameters', []):
        key_uri = param.get('keyUri', '')
        value = param.get('value', {})
        
        if 'reset-balance-amount' in key_uri:
            params['reset_balance_amount'] = value.get('number')
        elif 'reset-on-month' in key_uri:
            params['reset_on_month'] = value.get('uri', '')
        elif 'reset-on-day-of-month' in key_uri:
            params['reset_on_day'] = value.get('uri', '')
        elif 'precedence' in key_uri:
            params['precedence'] = value.get('number')
    
    return params


def extract_starting_balance_setup(policy_schedules):
    """
    Extract starting balance setup from policy schedules
    Equivalent to Workato Step 93-97: Extract "Starting Balance Set To" script parameters
    
    Args:
        policy_schedules: Response from GetDefaultTimeOffTypePolicyScheduleForUser API
        
    Returns:
        Dict with starting balance: starting_balance_amount, precedence
    """
    if not policy_schedules or len(policy_schedules) == 0:
        return {}
    
    policy_set = policy_schedules[0].get('policySet', {})
    scripts = policy_set.get('timeOffBalanceEventScripts', [])
    
    # Find "Starting Balance Set To" script
    starting_balance_script = next(
        (s for s in scripts if s.get('script', {}).get('name') == 'Starting Balance Set To'),
        None
    )
    
    if not starting_balance_script:
        return {}
    
    # Extract parameters
    params = {}
    for param in starting_balance_script.get('additionalParameters', []):
        key_uri = param.get('keyUri', '')
        value = param.get('value', {})

        if 'amount' in key_uri:
            params['starting_balance_amount'] = value.get('number')
        elif 'precedence' in key_uri:
            params['precedence'] = value.get('number')

    return params


# ============================================================================
# STEPS 99-130: Build Policy Entries List (No gsub, Pure Python Dicts)
# ============================================================================

def build_policy_entries_list(unique_effective_dates, effectivedates_mapper,
                              policy_template, startdate, 
                              accrual_setup, reset_setup):
    """
    Build complete policy entries list for all effective dates.
    
    Implements Workato Steps 99-130 in a single function:
    - Loop through unique effective dates (Step 99)
    - First iteration: Pro-ration calculation + starting balance (Steps 104-112)
    - Subsequent iterations: Full accrual + conditional starting balance (Steps 117-123)
    - Return list ready for API (Step 131)
    
    Args:
        unique_effective_dates: List of date strings ["YYYY-MM-DD", ...]
        effectivedates_mapper: List of {effectivedate, value, existingvalue, yoss}
        policy_template: Base policySet dict from API
        startdate: Employee start date (YYYY-MM-DD)
        accrual_setup: Dict with script_uri, accrue_on_month_uri
        reset_setup: Dict with script_uri, reset_on_month_uri
    
    Returns:
        List of policy entries ready for PutUserTimeOffAccountPolicySetSchedule API
    """
    if not unique_effective_dates or not policy_template:
        return []
    
    policy_entries = []
    
    for idx, effective_date_str in enumerate(unique_effective_dates):
        # Find mapper entry for this effective date
        mapper_entry = next(
            (e for e in effectivedates_mapper 
             if str(e.get('effectivedate')) == str(effective_date_str)),
            None
        )
        
        if not mapper_entry:
            continue
        
        accrual_value = mapper_entry.get('value', 0)
        
        # Build policy set based on iteration type
        if idx == 0:
            # FIRST ITERATION: Pro-ration + starting balance
            policy_set = _build_first_policy_set(
                policy_template,
                startdate,
                accrual_value,
                accrual_setup,
                reset_setup
            )
        else:
            # SUBSEQUENT ITERATIONS: Full accrual + conditional starting balance
            existing_value = mapper_entry.get('existingvalue', 0)
            policy_set = _build_subsequent_policy_set(
                policy_template,
                accrual_value,
                existing_value,
                accrual_setup,
                reset_setup
            )
        
        # Parse effective date
        try:
            effective_date_obj = datetime.strptime(effective_date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        
        # Build policy entry
        entry = {
            "effectiveDate": {
                "day": effective_date_obj.day,
                "month": effective_date_obj.month,
                "year": effective_date_obj.year
            },
            "description": f"Policy #{idx + 1}",
            "policySet": policy_set
        }
        
        policy_entries.append(entry)
    
    return policy_entries


def _build_first_policy_set(policy_template, startdate, accrual_value,
                            accrual_setup, reset_setup):
    """
    Build policy set for FIRST effective date with PRO-RATION.
    
    Workato Steps 104-112:
    - Calculate days in policy year
    - Pro-rate: (accrual / total_days) * days_in_policy
    - Round to 0.5 multiplier
    - Include starting balance with pro-rated amount
    
    Args:
        policy_template: Base policySet dict
        startdate: Employee start date
        accrual_value: Annual accrual amount
        accrual_setup: Dict with script info
        reset_setup: Dict with script info
    
    Returns:
        Dict with timeOffBalanceEventScripts array
    """
    # Step 106: Calculate days for pro-ration
    startdate_obj = datetime.strptime(startdate, "%Y-%m-%d")
    beginning_of_year = startdate_obj.replace(month=1, day=1)
    
    if startdate_obj > beginning_of_year:
        # Start after year beginning: days = (startdate + 12 months).beginning_of_year - startdate
        next_year_beginning = startdate_obj.replace(
            year=startdate_obj.year + 1, month=1, day=1
        )
        days_in_policy = (next_year_beginning - startdate_obj).days
    else:
        # Start on year beginning: days = end_of_year.yday (365 or 366)
        end_of_year = startdate_obj.replace(month=12, day=31)
        days_in_policy = end_of_year.timetuple().tm_yday
    
    # Step 108: Calculate total days in policy year (365 or 366)
    policy_year_end = startdate_obj.replace(
        year=startdate_obj.year + 1, month=1, day=1
    ) - timedelta(days=1)
    total_days_in_year = policy_year_end.timetuple().tm_yday
    
    # Step 109: Pro-ration calculation
    # starting_balance = (accrual / total_days) * days_in_policy
    starting_balance = (float(accrual_value) / float(total_days_in_year)) * float(days_in_policy)
    
    # Step 111: Round to 0.5 multiplier
    starting_balance_rounded = _round_to_half(starting_balance)
    
    # Deep copy template
    policy_set = json.loads(json.dumps(policy_template))
    
    # Build scripts array: Yearly Reset, Yearly Accrual, Starting Balance
    scripts = [
        _build_yearly_reset_script(reset_setup, accrual_value),
        _build_yearly_accrual_script(accrual_setup, accrual_value),
        _build_starting_balance_script(starting_balance_rounded)
    ]
    
    policy_set['timeOffBalanceEventScripts'] = scripts
    policy_set['timeOffValidationScripts'] = []
    
    return policy_set


def _build_subsequent_policy_set(policy_template, accrual_value, existing_value,
                                 accrual_setup, reset_setup):
    """
    Build policy set for SUBSEQUENT effective dates without pro-ration.
    
    Workato Steps 117-123:
    - Use full annual accrual (no pro-ration)
    - Conditional starting balance:
      - If existing_value == 0: skip starting balance script
      - If existing_value > 0: include starting balance with existing value
    
    Args:
        policy_template: Base policySet dict
        accrual_value: Annual accrual amount (full, no pro-ration)
        existing_value: Previous year accrual (for starting balance)
        accrual_setup: Dict with script info
        reset_setup: Dict with script info
    
    Returns:
        Dict with timeOffBalanceEventScripts array
    """
    # Deep copy template
    policy_set = json.loads(json.dumps(policy_template))
    
    # Build scripts array: Yearly Reset, Yearly Accrual, (optionally) Starting Balance
    scripts = [
        _build_yearly_reset_script(reset_setup, accrual_value),
        _build_yearly_accrual_script(accrual_setup, accrual_value)
    ]
    
    # Step 124: Add starting balance only if existing value > 0
    if existing_value and float(existing_value) > 0:
        scripts.append(_build_starting_balance_script(float(existing_value)))
    
    policy_set['timeOffBalanceEventScripts'] = scripts
    policy_set['timeOffValidationScripts'] = []
    
    return policy_set


def _build_yearly_reset_script(reset_setup, accrual_value):
    """
    Build Yearly Reset script with parameters.
    
    Workato Step 101-102, 105, 123: Build reset parameter JSON
    """
    return {
        "scriptTarget": {
            "uri": reset_setup.get('script_uri'),
            "name": "Yearly Reset",
            "description": "Reset balance once a year"
        },
        "additionalParameters": [
            {
                "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                "value": {"uri": "urn:replicon:time-off-policy-reset-option:carry-over-previous-balance-with-limit"}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:precedence",
                "value": {"number": 20.0}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                "value": {"number": float(accrual_value)}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                "value": {"uri": reset_setup.get('reset_on_month_uri')}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                "value": {"uri": "urn:replicon:monthly-frequency-start-day-option:1st"}
            }
        ]
    }


def _build_yearly_accrual_script(accrual_setup, accrual_value):
    """
    Build Yearly Accrual script with parameters.
    
    Workato Step 102, 104, 122: Build accrual parameter JSON
    """
    return {
        "scriptTarget": {
            "uri": accrual_setup.get('script_uri'),
            "name": "Yearly Accrual",
            "description": "Accrues time once per year."
        },
        "additionalParameters": [
            {
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {"number": float(accrual_value)}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:precedence",
                "value": {"number": 30.0}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                "value": {"uri": accrual_setup.get('accrue_on_month_uri')}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                "value": {"uri": "urn:replicon:monthly-frequency-start-day-option:1st"}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:proration-option",
                "value": {"uri": "urn:replicon:time-off-policy-proration-option:start-and-end-of-policy"}
            }
        ]
    }


def _build_starting_balance_script(amount):
    """
    Build Starting Balance script with parameters.
    
    Workato Step 112: Build starting balance parameter JSON
    """
    return {
        "scriptTarget": {
            "uri": None,  # Will be extracted from template if needed
            "name": "Starting Balance Set To",
            "description": "Set starting balance"
        },
        "additionalParameters": [
            {
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {"number": float(amount)}
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:precedence",
                "value": {"number": 10.0}
            }
        ]
    }


def _round_to_half(value):
    """
    Round value to nearest 0.5 multiplier.
    
    Workato Step 111 Logic:
    - If value % 0.5 == 0: return as-is
    - Else: if decimal > 50, round up to next integer; else round down to .50
    
    Args:
        value: Float value to round
    
    Returns:
        Float rounded to nearest 0.5
    """
    value = float(value)
    
    # Check if already a 0.5 multiple
    if value % 0.5 == 0:
        return value
    
    # Get integer and decimal parts
    int_part = int(value)
    decimal_part = value - int_part

    # If decimal > 0.5, round up to next integer
    if decimal_part > 0.5:
        return float(int_part + 1)
    else:
        # Round down to current integer + 0.5
        return float(int_part) + 0.5


def test_date_matching_for_policy_building(unique_effective_dates, effectivedates_mapper):
    """Debug helper to test date matching logic in policy building"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Testing date matching: {len(unique_effective_dates)} unique dates vs {len(effectivedates_mapper)} mapper entries")

    matches = []
    for i, effective_date in enumerate(unique_effective_dates):
        logger.info(f"Testing unique date #{i+1}: {effective_date}")

        found_match = False
        for j, mapper_entry in enumerate(effectivedates_mapper):
            mapper_date = mapper_entry.get('effectivedate', {})

            date_matches = (
                mapper_date.get('day') == effective_date.get('day') and
                mapper_date.get('month') == effective_date.get('month') and
                mapper_date.get('year') == effective_date.get('year')
            )

            if date_matches:
                matches.append({
                    'unique_date_index': i,
                    'effective_date': effective_date,
                    'mapper_entry_index': j,
                    'matched_mapper_entry': mapper_entry,
                    'status': 'MATCHED'
                })
                logger.info(f"  ✅ MATCHED with mapper entry #{j}: {mapper_date}")
                found_match = True
                break

        if not found_match:
            matches.append({
                'unique_date_index': i,
                'effective_date': effective_date,
                'status': 'NO_MATCH_FOUND'
            })
            logger.warning(f"  ❌ NO MATCH found for date: {effective_date}")

    unmatched_count = len([m for m in matches if m['status'] == 'NO_MATCH_FOUND'])

    return {
        'total_unique_dates': len(unique_effective_dates),
        'total_mapper_entries': len(effectivedates_mapper),
        'successful_matches': len(matches) - unmatched_count,
        'unmatched_dates': unmatched_count,
        'matches_detail': matches,
        'debug_summary': f"{len(matches) - unmatched_count}/{len(unique_effective_dates)} dates successfully matched"
    }


def find_mapper_matches_for_dates(unique_effective_dates, effectivedates_mapper):
    """Find mapper entries that match each unique effective date"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Finding mapper matches for {len(unique_effective_dates)} dates")

    matches = []
    unmatched_dates = []

    for i, effective_date in enumerate(unique_effective_dates):
        found_match = False

        # Recipe uses pluck(...).last: the LAST entry for a date wins
        for mapper_entry in reversed(effectivedates_mapper):
            mapper_date = mapper_entry.get('effectivedate', {})

            if (mapper_date.get('day') == effective_date.get('day') and
                mapper_date.get('month') == effective_date.get('month') and
                mapper_date.get('year') == effective_date.get('year')):

                matches.append({
                    'effective_date': effective_date,
                    'mapper_entry': mapper_entry,
                    'date_index': i
                })
                logger.info(f"Date {i+1}: {effective_date} -> MATCHED")
                found_match = True
                break

        if not found_match:
            unmatched_dates.append(effective_date)
            matches.append({
                'effective_date': effective_date,
                'mapper_entry': None,
                'date_index': i
            })
            logger.warning(f"Date {i+1}: {effective_date} -> NO MATCH")

    return {
        'matches': matches,
        'unmatched_count': len(unmatched_dates),
        'unmatched_dates': unmatched_dates,
        'total_matches': len([m for m in matches if m['mapper_entry'] is not None])
    }


def calculate_prorated_starting_balance(startdate_str, first_effective_date, annual_accrual):
    """
    Recipe steps 106-111: prorated starting balance for the FIRST policy entry.
    - proration days: startdate > today.boy ? days from startdate to next year's Jan 1
                      : days in the current year (365/366)
    - reference date: startdate when it is this year or later, else the first effective date
    - balance = round(accrual / days-in-reference-year * proration days), or 0 when the
      reference date IS Jan 1 of its year.
    """
    today = datetime.now()
    today_boy = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    start_dt = datetime.strptime(startdate_str, '%Y-%m-%d')

    if start_dt > today_boy:
        proration_days = ((start_dt + relativedelta(months=12)).replace(month=1, day=1) - start_dt).days
    else:
        proration_days = (((today + relativedelta(months=12)).replace(month=1, day=1)) - timedelta(days=1)).timetuple().tm_yday

    if start_dt > today_boy - timedelta(days=1):
        ref_date = start_dt
    else:
        ref_date = datetime(first_effective_date['year'], first_effective_date['month'], first_effective_date['day'])

    days_in_year = (((ref_date + relativedelta(months=12)).replace(month=1, day=1)) - timedelta(days=1)).timetuple().tm_yday

    if ref_date > ref_date.replace(month=1, day=1):
        # Ruby .round = half away from zero
        return int(float(annual_accrual) / days_in_year * proration_days + 0.5)
    return 0


def _substitute_policy_params(policy_template, accrual_amount, month_uri,
                              starting_balance_amount=None, include_starting_balance=False):
    """
    Clone the default policySet and substitute exactly what the recipe substitutes
    (steps 113 / 125-127): accrual amount, reset balance amount, accrue/reset month
    (the YOSS month), starting-balance amount (first policy only — removed otherwise).
    All other parameters are preserved from the template. Applies the recipe's
    null -> "effective" and "script" -> "scriptTarget" rewrites.
    """
    policy_set = json.loads(
        json.dumps(policy_template).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))

    scripts = []
    for script in policy_set.get('timeOffBalanceEventScripts', []):
        name = (script.get('scriptTarget') or {}).get('name')
        if name == 'Starting Balance Set To':
            if not include_starting_balance:
                continue  # recipe step 127 removes the script for subsequent policies
            for param in script.get('additionalParameters', []):
                if param.get('keyUri', '').endswith(':amount'):
                    param['value']['number'] = starting_balance_amount
        elif name == 'Yearly Accrual':
            for param in script.get('additionalParameters', []):
                key = param.get('keyUri', '')
                if key.endswith('accrual-annual-amount'):
                    param['value']['number'] = float(accrual_amount)
                elif key.endswith('accrue-on-month'):
                    param['value']['uri'] = month_uri
        elif name == 'Yearly Reset':
            for param in script.get('additionalParameters', []):
                key = param.get('keyUri', '')
                if key.endswith('reset-balance-amount'):
                    param['value']['number'] = float(accrual_amount)
                elif key.endswith('reset-on-month'):
                    param['value']['uri'] = month_uri
        scripts.append(script)

    policy_set['timeOffBalanceEventScripts'] = scripts
    return policy_set


def build_single_policy_entry(effective_date, mapper_match, policy_template, startdate,
                             accrual_setup, reset_setup, starting_balance_setup,
                             is_first_policy=False, service_month_uri=None):
    """Build a single policy entry (recipe steps 103-130)."""
    import logging
    logger = logging.getLogger(__name__)

    # Validate mapper match
    if not mapper_match or not mapper_match.get('mapper_entry'):
        logger.error(f"No mapper entry found for date: {effective_date}")
        return None

    policy_number = mapper_match.get('date_index', 0) + 1
    mapper_entry = mapper_match['mapper_entry']
    accrual_value = float(mapper_entry.get('value', '0'))

    if is_first_policy:
        # Recipe: FULL accrual in the accrual/reset scripts; the PRORATED amount
        # only feeds the Starting Balance script.
        starting_balance = calculate_prorated_starting_balance(startdate, effective_date, accrual_value)
        policy_set = _substitute_policy_params(
            policy_template, accrual_value, service_month_uri,
            starting_balance_amount=starting_balance, include_starting_balance=True)
    else:
        # Recipe: subsequent policies never carry a Starting Balance script.
        policy_set = _substitute_policy_params(
            policy_template, accrual_value, service_month_uri,
            include_starting_balance=False)

    return {
        'effectiveDate': effective_date,
        'description': f'Policy {policy_number}',
        'policySet': policy_set
    }


def build_remaining_policy_entries(remaining_dates, remaining_matches, policy_template,
                                  accrual_setup, reset_setup, starting_balance_setup,
                                  service_month_uri=None):
    """Build all remaining policy entries (after the first one)"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Building {len(remaining_dates)} remaining policy entries")

    policy_entries = []

    for i, (date, match) in enumerate(zip(remaining_dates, remaining_matches), 2):  # Start from policy #2
        policy_entry = build_single_policy_entry(
            effective_date=date,
            mapper_match=match,
            policy_template=policy_template,
            startdate=None,  # Not needed for subsequent policies
            accrual_setup=accrual_setup,
            reset_setup=reset_setup,
            starting_balance_setup=starting_balance_setup,
            is_first_policy=False,
            service_month_uri=service_month_uri
        )

        if policy_entry:
            policy_entries.append(policy_entry)
            logger.info(f"Added policy #{i} to list")
        else:
            logger.warning(f"Failed to build policy #{i}, skipping")

    logger.info(f"Successfully built {len(policy_entries)} remaining policy entries")
    return policy_entries


def calculate_prorated_accrual_simple(startdate, annual_accrual):
    """Calculate pro-rated accrual for partial year (simplified version)"""
    import logging
    from datetime import datetime
    logger = logging.getLogger(__name__)

    try:
        # Parse start date
        if isinstance(startdate, str):
            start_dt = datetime.strptime(startdate, '%Y-%m-%d')
        else:
            start_dt = startdate

        # Calculate next year beginning
        next_year = start_dt.year + 1
        next_year_start = datetime(next_year, 1, 1)

        # Calculate days in policy period
        total_days_in_year = 365
        days_in_policy = (next_year_start - start_dt).days

        if days_in_policy <= 0:
            logger.warning(f"Invalid days_in_policy: {days_in_policy}, using full accrual")
            return annual_accrual

        # Pro-rate calculation
        prorated = (annual_accrual / total_days_in_year) * days_in_policy

        # Round to nearest 0.5 using existing function
        rounded_prorated = _round_to_half(prorated)

        logger.info(f"Pro-ration calculation: {annual_accrual} * ({days_in_policy}/{total_days_in_year}) = {prorated} -> {rounded_prorated}")

        return rounded_prorated

    except Exception as e:
        logger.error(f"Error in pro-ration calculation: {e}, using full accrual")
        return annual_accrual


def build_policy_set_with_all_scripts(policy_template, accrual_amount, existing_value,
                                    accrual_setup, reset_setup, starting_balance_setup, include_starting_balance):
    """Build policy set with all required scripts"""
    import logging
    logger = logging.getLogger(__name__)

    scripts = []

    # 1. Add Yearly Reset script
    try:
        reset_script = build_yearly_reset_script_simple(policy_template, reset_setup)
        scripts.append(reset_script)
        logger.info("Added Yearly Reset script")
    except Exception as e:
        logger.error(f"Failed to add Yearly Reset script: {e}")

    # 2. Add Yearly Accrual script
    try:
        accrual_script = build_yearly_accrual_script_simple(policy_template, accrual_setup, accrual_amount)
        scripts.append(accrual_script)
        logger.info(f"Added Yearly Accrual script with amount: {accrual_amount}")
    except Exception as e:
        logger.error(f"Failed to add Yearly Accrual script: {e}")

    # 3. Conditionally add Starting Balance script
    if include_starting_balance:
        try:
            starting_balance_script = build_starting_balance_script_simple(policy_template, starting_balance_setup, existing_value)
            scripts.append(starting_balance_script)
            logger.info(f"Added Starting Balance script with amount: {existing_value}")
        except Exception as e:
            logger.error(f"Failed to add Starting Balance script: {e}")

    return {
        'timeOffBalanceEventScripts': scripts,
        'timeOffValidationScripts': []
    }


def build_yearly_reset_script_simple(policy_template, reset_setup):
    """Build yearly reset script from template and setup"""
    script_target = find_script_by_name(policy_template, 'Yearly Reset')

    return {
        'scriptTarget': script_target,
        'additionalParameters': [
            {'keyUri': 'urn:replicon:script-key:parameter:periodic-reset-option',
             'value': {'uri': 'urn:replicon:time-off-policy-reset-option:carry-over-previous-balance-with-limit'}},
            {'keyUri': 'urn:replicon:script-key:parameter:precedence',
             'value': {'number': reset_setup.get('precedence', 20.0)}},
            {'keyUri': 'urn:replicon:script-key:parameter:reset-balance-amount',
             'value': {'number': reset_setup.get('reset_balance_amount', 20.0)}},
            {'keyUri': 'urn:replicon:script-key:parameter:reset-on-day-of-month',
             'value': {'uri': reset_setup.get('reset_on_day', 'urn:replicon:monthly-frequency-start-day-option:1st')}},
            {'keyUri': 'urn:replicon:script-key:parameter:reset-on-month',
             'value': {'uri': reset_setup.get('reset_on_month', 'urn:replicon:month:january')}}
        ]
    }


def build_yearly_accrual_script_simple(policy_template, accrual_setup, accrual_amount):
    """Build yearly accrual script from template and setup"""
    script_target = find_script_by_name(policy_template, 'Yearly Accrual')

    return {
        'scriptTarget': script_target,
        'additionalParameters': [
            {'keyUri': 'urn:replicon:script-key:parameter:accrual-annual-amount',
             'value': {'number': accrual_amount}},
            {'keyUri': 'urn:replicon:script-key:parameter:accrue-on-month',
             'value': {'uri': accrual_setup.get('accrue_on_month', 'urn:replicon:month:january')}},
            {'keyUri': 'urn:replicon:script-key:parameter:accrue-on-day-of-month',
             'value': {'uri': accrual_setup.get('accrue_on_day', 'urn:replicon:monthly-frequency-start-day-option:1st')}},
            {'keyUri': 'urn:replicon:script-key:parameter:precedence',
             'value': {'number': accrual_setup.get('precedence', 30.0)}},
            {'keyUri': 'urn:replicon:script-key:parameter:proration-option',
             'value': {'uri': 'urn:replicon:time-off-policy-proration-option:start-and-end-of-policy'}}
        ]
    }


def build_starting_balance_script_simple(policy_template, starting_balance_setup, balance_amount):
    """Build starting balance script from template and setup"""
    script_target = find_script_by_name(policy_template, 'Starting Balance Set To')

    return {
        'scriptTarget': script_target,
        'additionalParameters': [
            {'keyUri': 'urn:replicon:script-key:parameter:amount',
             'value': {'number': balance_amount}},
            {'keyUri': 'urn:replicon:script-key:parameter:precedence',
             'value': {'number': starting_balance_setup.get('precedence', 10.0)}}
        ]
    }


def find_script_by_name(policy_template, script_name):
    """Find script in template by name and return script target"""
    scripts = policy_template.get('timeOffBalanceEventScripts', [])

    for script in scripts:
        if script.get('script', {}).get('name') == script_name:
            return {
                'uri': script['script']['uri'],
                'name': script['script']['name'],
                'description': script['script'].get('description', '')
            }

    # Return fallback if not found
    return {
        'uri': None,
        'name': script_name,
        'description': f'Script for {script_name}'
    }


def dict_date_to_datetime(dict_date):
    """
    Convert Replicon API date object {year, month, day} to Python date.

    Args:
        dict_date (dict): Dictionary with 'year', 'month', 'day' keys

    Returns:
        date: Python date object
    """
    return datetime.strptime(str(dict_date['year']) + "/" + str(dict_date['month']) + "/" + str(dict_date['day']), "%Y/%m/%d").date()


def get_relevant_historical_policies(existing_timeoff_policysetschedule, effective_date_derived, current_date_format):
    """
    Filter existing policy set schedule to return only historical policies.
    Historical policies are those with effective dates before the target date.

    Args:
        existing_timeoff_policysetschedule (list): List of policy schedule entries
        effective_date_derived (str): Target date string (YYYY-MM-DD format)
        current_date_format (str): Date format string (e.g., '%Y-%m-%d')

    Returns:
        list: Filtered list of historical policy entries with JSON transformations applied
    """
    if bool(existing_timeoff_policysetschedule and existing_timeoff_policysetschedule[0] and existing_timeoff_policysetschedule[0]['description']):
        cutoff = datetime.strptime(effective_date_derived, current_date_format).date()
        historical = [item for item in existing_timeoff_policysetschedule
                      if dict_date_to_datetime(item['effectiveDate']) < cutoff]

        return json.loads(json.dumps(historical).replace('null', '"effective"').replace(
            '"script"', '"scriptTarget"'))

    return []


def build_shift_worker_policy_schedule(policy_data, workshift_date_parsed, workshift_changeddate):
    """
    Implement Workato steps 40-48: Build shift worker policy schedule.

    This function implements the exact business logic from the Workato recipe:
    - Step 40: Create timeoffpolicy list
    - Step 41: ForEach loop over policy data
    - Step 42-43: IF offsetValue == 0, add policy with workshift_changeddate
    - Step 44-45: IF offsetValue == 1, add policy with next year Jan 1st
    - Step 46-47: Format with string transformations

    Args:
        policy_data (list): Response from GetDefaultTimeOffPolicySetScheduleForTimeOffType
        workshift_date_parsed (dict): Parsed workshift date {day, month, year}
        workshift_changeddate (str): Original workshift changed date string (YYYY-MM-DD)

    Returns:
        list: Formatted policy schedule entries ready for API submission
    """
    import json
    from datetime import datetime, timedelta

    # Step 40: Create timeoffpolicy list
    timeoff_policy = []

    if not policy_data:
        return timeoff_policy

    # Step 41: ForEach loop over policy data (equivalent to Workato's foreach)
    for i, policy_item in enumerate(policy_data):

        # Extract offset value for conditional logic
        start_offset = policy_item.get('startOffset', {})
        offset_value = start_offset.get('offsetValue')
        policy_set = policy_item.get('policySet', {})

        if offset_value is None:
            continue

        # Step 42-43: IF offsetValue == 0 (use workshift_changeddate)
        if offset_value == 0:
            # Format workshift_changeddate as MM/DD/YYYY for description
            try:
                date_obj = datetime.strptime(workshift_changeddate, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%m/%d/%Y')

                policy_entry = {
                    'description': f"Effective on {formatted_date}",
                    'effectiveDate': {
                        'day': workshift_date_parsed['day'],
                        'month': workshift_date_parsed['month'],
                        'year': workshift_date_parsed['year']
                    },
                    'policySet': policy_set
                }
                timeoff_policy.append(policy_entry)

            except Exception as e:
                continue

        # Step 44-45: IF offsetValue == 1 (use next year January 1st)
        elif offset_value == 1:

            # Recipe: 01/01 of (today + 12 months)
            today = datetime.now()
            next_year = (today + relativedelta(months=12)).year

            policy_entry = {
                'description': f"Effective on 01/01/{next_year}",
                'effectiveDate': {
                    'day': 1,
                    'month': 1,
                    'year': next_year
                },
                'policySet': policy_set
            }
            timeoff_policy.append(policy_entry)

        else:
            print(f"⚠️ Unexpected offset value: {offset_value}, skipping")

    # Step 47: Apply string transformations (Workato gsub operations)
    # Convert to JSON string, apply transformations, then parse back
    try:
        # Convert to JSON string
        json_string = json.dumps(timeoff_policy, indent=None, separators=(',', ':'))

        # Apply Workato transformations:
        # 1. null → "effective"
        json_string = json_string.replace('"null"', '"effective"')
        json_string = json_string.replace('null', '"effective"')

        # 2. "script" → "scriptTarget"
        json_string = json_string.replace('"script"', '"scriptTarget"')

        # 3. Fix JSON formatting issues
        json_string = json_string.replace('[{keyUri":', '[{"keyUri":')
        json_string = json_string.replace('\\n', '')
        json_string = json_string.replace('\\"', '"')

        # Parse back to Python objects
        transformed_policy = json.loads(json_string)

        return transformed_policy

    except Exception as e:
        return timeoff_policy
