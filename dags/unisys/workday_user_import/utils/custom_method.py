"""
Custom Methods Utility - Unisys Workday User Import

Provides business logic and data transformation functions for user import processing.
This module contains helper functions for data mapping, validation, log formatting,
and configuration generation used throughout the user import workflow.

Key features:
    - Date manipulation and formatting
    - User data mapping and transformation
    - Supervisor validation and message generation
    - Log formatting and consolidation
    - Configuration payload generation
    - Business rule validation
    - Location and timezone handling
    - Holiday calendar determination

Constants:
    DATE_FORMAT: Standard date format "%m/%d/%Y"
    LEVEL2_HOLIDAY_CALENDAR_FOR: Countries requiring state-level holiday calendars
    LEVEL2_STATES: Specific states with dedicated holiday calendars

Functions:
    get_today_date(): Get current date as Replicon date dict
    clean_pipe_string(input_str): Clean pipe-separated strings
    get_payload_locations(dag_run): Extract location payloads
    get_payload_user_data(): Extract user data from query results
    get_old_profile_update_log(dag_run): Generate old profile update logs
    derive_mapper_values(user_data): Map user data to timesheet/approval settings
    get_process_users_conf(item): Build config for process_users DAG
    get_process_groups_conf(): Build config for process_groups DAG
    get_process_new_users_conf(dag_run): Build config for new user DAG
    get_process_update_users_conf(dag_run): Build config for update user DAG
    get_add_user_message(): Generate add user completion message
    get_add_user_severity(): Determine add user severity level
    get_update_user_message(): Generate update user completion message
    get_update_user_severity(): Determine update user severity level
    get_supervisor_message(action, dag_run): Generate supervisor assignment message
    validate_enddate(dag_run): Validate end date is after start date
    if_end_date_in_past(dag_run): Check if end date is in the past
    can_user_profile_enable(dag_run): Check if user can be enabled
    get_out_of_scope_location(user_data, locations): Check location scope
    validate_supervisor_changed(): Validate supervisor change
    get_supervisor_data_with_manager_id(dag_run): Build supervisor search payload
    get_task_state(task_id): Get task execution state
    load_records(log_artifact): Load log records from artifact
    do_format_logs(dag_run): Format and consolidate all logs
"""
import pendulum
from datetime import datetime
from ast import literal_eval
import rail
from unisys.workday_user_import.mappers.user_sync_mapper import user_sync_mapper
from unisys.workday_user_import.mappers.timezone_mapper import timezone_mapper
from unisys.workday_user_import.mappers.workweek_mapper import workweek_mapper
from unisys.workday_user_import.mappers.project_mapper import project_mapper

null = None

DATE_FORMAT = "%m/%d/%Y"
LEVEL2_HOLIDAY_CALENDAR_FOR = ['india', 'canada', 'australia']
# LEVEL2_STATES = ['bengaluru', 'hyderabad', 'mumbai', 'gurgaon', 'alberta', 'british columbia', 'ontario', 'saskatchewan', 'manitoba', 'nova scotia', 'new brunswick', 'prince edward island', 'newfoundland', 'labrador', 'quebec', 'south australia', 'tasmania', 'western australia', 'queensland', 'victoria', 'new south wales']
MANDATORY_KEY = [
    'employee_id','login_name','first_name','last_name','location', 'user_type', 'companycode_costcenter', 'schedule',
    'start_date', 'user_status'
]

def get_today_date():
    """
    Get current date as Replicon date dictionary.

    Returns:
        dict: Date dictionary with keys 'year', 'month', 'day'
            - year (int): Current year
            - month (int): Current month (1-12)
            - day (int): Current day of month (1-31)

    Example:
        >>> get_today_date()
        {'year': 2025, 'month': 10, 'day': 14}
    """
    now = pendulum.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def clean_pipe_string(input_str):
    """
    Remove empty or whitespace-only segments from pipe-separated string.
    
    Args:
        input_str (str): Pipe-separated string
        
    Returns:
        str: Cleaned pipe-separated string with empty segments removed
        
    Examples:
        >>> clean_pipe_string("Canada|Alberta|CA0108")
        'Canada|Alberta|CA0108'
        
        >>> clean_pipe_string("Canada||CA0108")
        'Canada|CA0108'
        
        >>> clean_pipe_string("Canada| |CA0108")
        'Canada|CA0108'
    """
    # Split by pipe, strip whitespace, filter out empty strings
    parts = [part.strip() for part in input_str.split('|')]
    cleaned_parts = [part for part in parts if part]
    
    # Join back with pipe
    return '|'.join(cleaned_parts)

def get_payload_locations(dag_run):
    payload_location = rail.load_all_records(rail.result('query_valid_delta_records_locations'))
    payload_locations = []
    for rec in payload_location:
        payload_locations.append({
            "full_path": clean_pipe_string(rec['location']),
            "locationcode": rec['location_description'],
        })
    return payload_locations


def strip_extra_spaces(value):
    parts = [part.strip() for part in value.split('|')]
    return '|'.join(parts)

def get_payload_user_data():
    payload_user = rail.load_all_records(rail.result('query_user_data'))
    if len(payload_user) == 1:
        user_data = payload_user[0]
        user_data['location'] = clean_pipe_string(user_data['location'])
        user_data['user_type'] = strip_extra_spaces(user_data['user_type'])
        user_data['companycode_costcenter'] = strip_extra_spaces(user_data['companycode_costcenter'])
        return {
            "user_data": user_data,
            "old_data": null
        }
    user_data = payload_user[0] if not payload_user[0]['end_date'] else payload_user[1]
    user_data['location'] = clean_pipe_string(user_data['location'])
    user_data['user_type'] = strip_extra_spaces(user_data['user_type'])
    user_data['companycode_costcenter'] = strip_extra_spaces(user_data['companycode_costcenter'])
    return {
        "user_data": user_data,
        "old_data": payload_user[0] if payload_user[0]['end_date'] else payload_user[1]
    }

def get_old_profile_update_log(dag_run):
    old_data = rail.result('get_user_payload_data')['old_data']
    return {
        "lastname": old_data['last_name'],
        "firstname": old_data['first_name'],
        "loginname": old_data['login_name'],
        "employeeid": f"{dag_run.conf['employee_id']}",
        "manager": old_data['supervisor_id'],
        "userstatus": old_data['user_status'],
        "co_costcenter": old_data['cost_center_description'],
        "location": old_data['location_description'],
        "action": "Update",
        'status': 'Success' if old_data else 'Skipped',
        'details': f"User's old profile {dag_run.conf['employee_id']} is disabled" if old_data else f"User's old profile {dag_run.conf['employee_id']} update skipped since profile does not present in Replicon.",
    }

def derive_mapper_values(user_data):
    """
    Find best matching mapper row and return derived timesheet/approval values.
    Priority: Exact matches > List matches > All matches > not_in matches
    """

    def is_all_or_na(value):
        return isinstance(value, str) and value.lower() in {"all", "na"}

    def is_not_in(value):
        return isinstance(value, dict) and "not_in" in value

    def is_list_match(value):
        return isinstance(value, list)

    def matches_field(user_value, mapper_value):
        """Check if user value matches mapper field."""
        if is_all_or_na(mapper_value):
            return True
        if isinstance(mapper_value, str):
            return user_value == mapper_value
        if isinstance(mapper_value, list):
            return user_value in mapper_value
        if is_not_in(mapper_value):
            return user_value not in mapper_value["not_in"]
        return False

    def classify_match(mapper_value):
        """Return type index for scoring: exact=0, list=1, all=2, not_in=3"""
        if is_all_or_na(mapper_value):
            return 2
        if is_list_match(mapper_value):
            return 1
        if is_not_in(mapper_value):
            return 3
        return 0

    def get_match_score(user_data, mapper_row):
        """Calculate match score with priority: exact > list > all > not_in"""
        counts = [0, 0, 0, 0]  # exact, list, all, not_in

        def process_field(user_val, mapper_val):
            if not matches_field(user_val, mapper_val):
                return False
            counts[classify_match(mapper_val)] += 1
            return True

        # Location Level 1 + 2
        location_parts = [p.lower() for p in user_data["location"].split("|")]
        if not process_field(location_parts[0], mapper_row["Location Level 1"].lower()
                             if isinstance(mapper_row["Location Level 1"], str)
                             else mapper_row["Location Level 1"]):
            return (False, 0, 0, 0, 0)

        if len(location_parts) > 1:
            loc2 = mapper_row["Location Level 2"]
            if is_not_in(loc2):
                loc2 = {"not_in": [v.lower() for v in loc2["not_in"]]}
            elif isinstance(loc2, list):
                loc2 = [v.lower() for v in loc2]
            elif isinstance(loc2, str):
                loc2 = loc2.lower()
            if not process_field(location_parts[1], loc2):
                return (False, 0, 0, 0, 0)

        # Company Code
        if not process_field(user_data["companycode_costcenter"].split("|")[0],
                             mapper_row["Company Code"]):
            return (False, 0, 0, 0, 0)

        # Job Code
        if not process_field(user_data["job_code"], mapper_row["Job Codes"]):
            return (False, 0, 0, 0, 0)

        # Premium Pay Eligibility
        if not process_field(user_data["premium_pay_eligible"].lower(),
                             mapper_row["Premium Pay Eligibility"]):
            return (False, 0, 0, 0, 0)

        # User Type levels
        usertype_parts = user_data["user_type"].split("|")
        for i, key in enumerate(["User Type - Level 1",
                                 "User Type - Level 2",
                                 "User Type - Level 3"][:len(usertype_parts)]):
            if not process_field(usertype_parts[i], mapper_row[key]):
                return (False, 0, 0, 0, 0)

        return (True, *counts)

    # Best match selection
    best_match, best_score = None, (False, -1, -1, float("inf"), float("inf"))
    for mapper_row in user_sync_mapper:
        is_valid, exact, list_, all_, not_in_ = get_match_score(user_data, mapper_row)
        if not is_valid:
            continue

        current = (True, exact, list_, all_, not_in_)
        # Priority: maximize exact > list, minimize all > not_in
        if (
            current[1] > best_score[1]
            or (current[1] == best_score[1] and current[2] > best_score[2])
            or (current[1:3] == best_score[1:3] and current[3] < best_score[3])
            or (current[1:4] == best_score[1:4] and current[4] < best_score[4])
        ):
            best_score, best_match = current, mapper_row

    if best_match:
        print(f"user payload data: {user_data}")
        print(f"matched mapper value: {best_match}")
        print(
            f"match score - exact: {best_score[1]}, list: {best_score[2]}, all: {best_score[3]}, not_in: {best_score[4]}"
        )
        return {
            "timesheet_template": best_match.get("Timesheet Template", ""),
            "timesheet_approval_path": best_match.get("Timesheet Approval Path", ""),
            "timesheet_period": best_match.get("Timesheet Period", ""),
            "pay_rule": best_match.get("Pay Rule", ""),
            "time_types": best_match.get("Time Types", ""),
            "work_week": best_match.get("Work Week", ""),
        }

    return None

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
                'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_permission_sets'),'displayText',"Employee",'uri')
            })
        return permissionsets

def get_process_users_conf(item, config):
    return {
        'licences': get_licences_to_be_assigned(config),
        'permissionsetdetails': get_all_permissionseturis(),
        "employee_id": item['employee_id'],
        "replicon_location_details": rail.write_json_artifact(rail.result('get_updated_location_details')),
        "replicon_usertypes_details": rail.write_json_artifact(rail.result('get_updated_employeetype_groups_data')),
        "replicon_division_details": rail.write_json_artifact(rail.result('get_enabled_co_costcenters')),
        "replicon_user_udfs": rail.result('get_user_customfields'),
        "replicon_permission_sets": rail.write_json_artifact(rail.result('get_permission_sets')),
        "replicon_payrules": rail.write_json_artifact(rail.result('get_all_payrule_scripts')),
        "replicon_policy_sets": rail.write_json_artifact(rail.result('get_all_policy_sets')),
        "replicon_ts_approval_paths": rail.write_json_artifact(rail.result('get_timesheet_approval_paths')),
        "replicon_all_timezones": rail.write_json_artifact(rail.result('get_all_timezones')),
        "replicon_office_schedule": rail.write_json_artifact(rail.result('get_updated_all_office_schedule')),
        "replicon_user_status_dropdown": rail.result('get_all_user_status_dropdowns'),
        "replicon_leave_type_dropdown": rail.result('get_all_leave_type_dropdowns'),
        "replicon_ts_period_list": rail.write_json_artifact(rail.result('get_all_timesheet_period_list')),
        "replicon_holiday_calendars": rail.write_json_artifact(rail.result('get_all_holiday_calendars')),
        "replicon_purchase_order_ids": rail.write_json_artifact(rail.result('get_all_enabled_purchase_order')),
        "admin_projects": rail.write_json_artifact(rail.result('get_project_details')),
        "replicon_activity_uris": rail.result('get_all_activity_uris'),
        'supervisor_log': rail.result('process_supervisor_log'),
    }

def get_process_groups_conf():
    return {
        "replicon_location_details": rail.write_json_artifact(rail.result('get_location_details')),
        "replicon_usertypes_details": rail.write_json_artifact(rail.result('get_employeetype_groups_data')),
        "replicon_division_details": rail.write_json_artifact(rail.result('get_enabled_co_costcenters')),
        
    }

def _get_all_records(artifact):
    return rail.load_all_records(artifact)

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_process_new_users_conf(dag_run):
    """
    Build configuration dictionary for process_new_users child DAG.

    Assembles all required configuration data for creating a new user including
    user attributes, mapped timesheet settings, timezone, holiday calendar,
    and references to all prerequisite data artifacts.

    Args:
        dag_run: DAG run context containing:
            - conf: User data and prerequisite artifacts
            - Requires results from get_user_payload_data

    Returns:
        dict: Complete configuration for new user processing including:
            - All user attributes (name, email, employee_id, etc.)
            - Mapped values (timesheet settings, approval paths)
            - URIs for all organizational groups
            - Timezone and holiday calendar assignments
            - Artifact references for prerequisite data
            - Log artifact references

    Note:
        This function performs extensive data lookups and mapping to resolve
        names to URIs and determine appropriate settings based on user attributes.
    """
    user_payload_data = rail.result('get_user_payload_data')['user_data']
    mapper_data = derive_mapper_values(user_payload_data)
    location = user_payload_data['location'].split('|')
    def timezone_details():
        """
        Determine timezone for user based on location country.

        Returns:
            dict: Timezone details with 'tz' and 'uri' keys
        """
        country = location[0].lower()
        tz = ''
        uri = ''
        for item in timezone_mapper:
            if item['country'].lower() == country:
                tz = item['timezone']
                break
        if tz:
            for rec in _get_all_records(dag_run.conf['replicon_all_timezones']):
                if rec['displayText'] == tz:
                    uri = rec['uri']
                    break
        return {
            'tz': tz,
            'uri': uri
        }
    def get_holiday_calander_uri():
        user_type = user_payload_data['user_type'].split('|')
        country = location[0].lower()
        state = location[1].lower() if len(location) > 1 else ''
        if (not user_type or user_type[0] != 'Employee'):
            return ''
        search_with = country if country not in LEVEL2_HOLIDAY_CALENDAR_FOR else state
        if state == 'puerto rico':
            search_with = state
        for rec in _get_all_records(dag_run.conf['replicon_holiday_calendars']):
            if rec['displayText'].lower() == search_with:
                return rec['uri']
        return ''

    return {
        **user_payload_data,
        **dag_run.conf,
        **{
            'holiday_calander_uri': get_holiday_calander_uri(),
            'timezone': timezone_details()['tz'],
            'pay_rule': mapper_data['pay_rule'] if mapper_data else '',
            'work_week': mapper_data['work_week'] if mapper_data else '',
            'work_week_uri': rail.find_first_by_attr_and_get_attr(workweek_mapper,'value', mapper_data[
                'work_week'].split()[0].lower(),'uri') if mapper_data else '',
            'time_types': [tt.strip() for tt in mapper_data['time_types'].split(',')] if mapper_data else [],
            'timezoneuri': timezone_details()['uri'],
            'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_permission_sets']),'displayText',"Supervisor",'uri'),
            'payrule_uri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_payrules']),'displayText',mapper_data[
                'pay_rule'],'uri') if mapper_data else '',
            'country_name': location[0],
            'location_uri':rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_location_details']),'fullpath',user_payload_data['location'],'uri'),
            'user_type_uri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_usertypes_details']),'fullpath',user_payload_data['user_type'],'uri'),
            'companycode_costcenter_uri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_division_details']),'fullpath',user_payload_data['companycode_costcenter'],'uri'),
            'schedule_uri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_office_schedule']),'displayText',user_payload_data['schedule'],'uri'),
            'purchase_order_id_uri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_purchase_order_ids']),'displayText',user_payload_data['purchase_order_id'],'uri'),
            'user_status_value_uri': rail.find_first_by_attr_and_get_attr(dag_run.conf['replicon_user_status_dropdown'],'displayText',user_payload_data['user_status'],'uri'),
            'leave_type_value_uri': rail.find_first_by_attr_and_get_attr(dag_run.conf['replicon_leave_type_dropdown'],'displayText',user_payload_data['leave_type'],'uri'),
            'timesheetperiod':mapper_data['timesheet_period'] if mapper_data else '',
            'timesheet_period_uri':rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf['replicon_ts_period_list']), 'name', mapper_data[
                'timesheet_period'],'uri') if mapper_data else '',
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf["replicon_policy_sets"]),'displayText',mapper_data[
                'timesheet_template'],'uri') if mapper_data else '',
            'timesheettemplate': mapper_data['timesheet_template'] if mapper_data else '',
            'timesheetapprovalpath': mapper_data['timesheet_approval_path'] if mapper_data else '',
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(_get_all_records(dag_run.conf["replicon_ts_approval_paths"]),'displayText', mapper_data[
                'timesheet_approval_path'],'uri') if mapper_data else '',
            'mapper_data': mapper_data,
            'user_log': rail.result('process_user_log'),
            'project_user_log': rail.result('process_project_log')
        }
    }

def get_supervisor_permission_uri():
    return rail.find_first_by_attr_and_get_attr(rail.result('get_permission_sets'),'displayText',"Supervisor",'uri')

def get_add_user_message():
    # pylint: disable=too-many-return-statements
    exception_logs = rail.result('add_new_user', 'exception_logs')
    if get_task_state('log_user_supervisor_same') == 'success':
        return "Employee and Supervisor is same;" + rail.smartjoin_by_delim(exception_logs, ";")
    if not exception_logs:
        return "User Added Successfully"
    return "User Partially Added;"+ rail.smartjoin_by_delim(exception_logs, ";")

def get_add_user_severity():
    if get_task_state('log_user_supervisor_same') == 'success'\
        or rail.result('add_new_user', 'exception_logs'):
        return 'Exception'
    return 'Success'


def validate_enddate(dag_run):
    if dag_run.conf['start_date'] and dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) >= datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)
    return False

def if_end_date_in_past(dag_run):
    current = now = pendulum.now().strftime(DATE_FORMAT)
    if not dag_run.conf['end_date']:
        return False
    return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) < datetime.strptime(current, DATE_FORMAT)

def get_process_update_users_conf(dag_run):
    return {
        **get_process_new_users_conf(dag_run),
        **{
            'useruri': rail.result('get_user_by_empl_id')[0]['userDetails']['uri'],
            'user_data': rail.write_json_artifact(rail.result('get_user_by_empl_id'))
        }
    }

def get_update_user_message():
    # pylint: disable=too-many-return-statements
    exception_logs = rail.result('update_existing_user', 'exception_logs')
    if get_task_state('log_user_supervisor_same') == 'success':
        return "Employee and Supervisor is same;"+ rail.smartjoin_by_delim(exception_logs, ";")
    if not exception_logs:
        return "User Updated Successfully"
    return "User Partially Updated;"+ rail.smartjoin_by_delim(exception_logs, ";")

def get_update_user_severity():
    if get_task_state('log_user_supervisor_same') == 'success'\
        or rail.result('update_existing_user', 'exception_logs'):
        return 'Exception'
    return 'Success'

def get_out_of_scope_location(user_data, locations):
    if user_data['user_type'].split('|')[0] != 'Contingent Worker':
        return False
    location_parts = user_data['location'].split('|')
    if location_parts[0].lower() in locations:
        return True
    if len(location_parts) > 1 and location_parts[1].lower() == 'puerto rico':
        return True

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
    # pylint: disable=too-many-return-statements
    exception_log = dag_run.conf['exception_logs'] if dag_run.conf.get('exception_logs') else []
    if get_task_state('log_supervisor_not_present') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + \
            ',Supervisor not present in replicon;'+ rail.smartjoin_by_delim(exception_log, ";")
    return f"""User {('Added Successfully' if action=='add' else 'Updated Successfully')
        if not exception_log else ('Partially Added,'if action=='add' else 'Partially Updated,') + rail.smartjoin_by_delim(exception_log, ";")}"""

def get_supervisor_severity():
    if get_task_state('log_supervisor_not_present') == 'success':
        return 'Exception'
    return 'Success'

def load_records(log_artifact):
    """Load all records from a log artifact"""
    return rail.load_all_records(log_artifact)

def do_format_logs(dag_run):
    """
    Format and consolidate logs from multiple DAG runs

    Args:
        dag_run: DAG run context containing log artifacts

    Returns:
        List of formatted log records
    """
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
    
    def get_log_status(entry_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], entry_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"
    
    final_log_records = []

    # merge_rows = list(map(lambda x: {
    #     'merge_rows': f"{x['properties']['employeeid']}"
    #     }, log_records))

    # final_data = list({f"{value['merge_rows']}": value for value in merge_rows}.values())

    # #pylint: disable=cell-var-from-loop
    # for item in final_data:
    #     entries_log = list(
    #         filter(lambda x: 
    #                (x['properties'].get('employeeid', '') == item['merge_rows'].split('|')[0]), log_records))
    #     if len(entries_log) > 0:
    #         first = entries_log[0]
    #         empl_id = first['properties']['employeeid']
    #         empl_id = empl_id.replace('_old', '') if empl_id.endswith('_old') else empl_id
    #         final_log_records.append({
    #             'loginname': first['properties']['loginname'],
    #             'firstname': first['properties']['firstname'],
    #             'lastname': first['properties']['lastname'],
    #             'employeeid': empl_id,
    #             'manager': first['properties']['manager'],
    #             'action': first['properties']['action'],
    #             'status': get_log_status(entries_log),
    #             "details":  '; '.join(list(set(map(lambda x: x['properties'].get('details'), entries_log)))),
    #             'ecid': first['ecid'],
    #         })

    final_log_records = list(map(lambda log: {
        **{
            'ecid': log['ecid']
        },
            **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: x['status'] == 'Skipped', final_log_records ))))

    return  final_log_records

def get_admin_project_codes(companycode_costcenter, user_type):
    company_code = companycode_costcenter.split('|')[0]
    usertype = user_type.split('|')[0]
    projects = list(filter(lambda x: x['company_code'] == company_code and x['user_type'] == usertype, project_mapper))
    return list(map(lambda item:{
        'code':item['admin_project_code']
    }, projects))

def get_old_admin_project_uris(dag_run, project_codes):
    prjct_cd_hash = rail.load_all_records(dag_run.conf['admin_projects'])
    prjct_cd_hash = prjct_cd_hash[0] if prjct_cd_hash else {}
    return list(map(lambda item:{
                    "code": item['code'],
                    "uri": prjct_cd_hash.get(item['code']),
                }, project_codes))

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
    return strng.split('|')[0]

def has_company_code_changed(dag_run):
    effective_data = rail.result('get_effective_co_code_usertype')
    effective_company_code = effective_data['division']
    effective_usertype = effective_data['usertype']
    
    rplcn_divisions = rail.load_all_records(dag_run.conf['replicon_division_details'])
    rplcn_usertypes = rail.load_all_records(dag_run.conf['replicon_usertypes_details'])
    
    new_co_code = dag_run.conf['companycode_costcenter']
    new_usertype = dag_run.conf['user_type']
    
    response = {"new_projects": [], "old_projects": [], "project_codes": []}
    
    
    # If no effective values exist, add new projects
    if not (effective_company_code and effective_usertype) and new_co_code:
        response['new_projects'] = get_admin_project_codes(new_co_code, new_usertype)
        response['project_codes'] = response['new_projects']
        return response

    # Get current values
    current_co_code = rail.find_first_by_attr_and_get_attr(rplcn_divisions, 'uri', effective_company_code['uri'])
    current_usertype = rail.find_first_by_attr_and_get_attr(rplcn_usertypes, 'uri', effective_usertype['uri'])
    current_co_code = current_co_code or {}
    current_usertype = current_usertype or {}
    
    # Check if changed
    co_code_changed = current_co_code.get('fullpath', '') != new_co_code
    usertype_changed = get_zero_indx_value(current_usertype.get('fullpath', '')) != get_zero_indx_value(new_usertype)
    
    if co_code_changed or usertype_changed:
        response['old_projects'] = get_admin_project_codes(
            current_co_code.get('fullpath', ''), 
            current_usertype.get('fullpath', '')
        )
        response['new_projects'] = get_admin_project_codes(new_co_code, new_usertype)
    
    response['project_codes'] = response['old_projects'] + response['new_projects']
    return response

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key in MANDATORY_KEY:
        if not item[payload_key]:
            payload_key = ' '.join(word.capitalize() for word in payload_key.split('_'))
            missing_fields.append(payload_key)
    log_msg = rail.smartjoin_by_delim(missing_fields, ";")
    log_msg = f"mandatory field(s) {log_msg} is not present in payload"
    return log_msg

def get_unique_project_codes():
    unique_codes = list(set([item["admin_project_code"] for item in project_mapper]))
    return [{"code": code} for code in unique_codes]

def get_resources_to_add(dag_run, batch_size=400):
    user_dict_list = rail.load_all_records(rail.result('query_resource_data'))
    resource_uris = [user_dict["useruri"] for user_dict in user_dict_list]
    result = []
    for task_uri in dag_run.conf['tasks']:
        for i in range(0, len(resource_uris), batch_size):
            batch = resource_uris[i:i + batch_size]
            result.append({
                "taskUri": task_uri,
                "uris": batch 
            })

    
    return result

def get_project_records():
    project_logs = rail.result('gather_project_logs')
    artifacts = []

    if project_logs:
        if isinstance(project_logs, list):
            artifacts.extend(project_logs)
        elif isinstance(project_logs, str) and project_logs[0] == '[':
            project_logs = literal_eval(project_logs)
            artifacts.extend(project_logs)
        else:
            artifacts.append(project_logs)
    responses = []
    if artifacts:
        for log in artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                responses.extend(each_log_records)
    final_project_records = list(map(lambda log: {
         **dict(log['properties'].items()),
        }, responses))
    return final_project_records
