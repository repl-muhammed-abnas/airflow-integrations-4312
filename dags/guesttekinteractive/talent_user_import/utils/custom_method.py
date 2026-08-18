"""
Custom Methods - GuestTek Talent User Import Integration

Business logic helper functions for the GuestTek Talent User Import integration.
These functions handle user categorization, validation, data transformation,
and complex business rule implementation.

Key features:
    - User categorization (new vs update vs disable)
    - Mapper validation
    - Date validation
    - Supervisor validation
    - Log formatting
    - Delta detection

Functions:
    categorize_users: Categorize users into new, update, disable, skip
    validate_mapper_lookup: Validate location/department in mapper
    is_valid_date: Check if date is valid (not 0000-00-00)
    get_user_status: Determine user status from Talent data
    format_logs: Format logs for CSV output
    get_effective_division_or_user: Get effective group from schedule
"""
import pendulum
from datetime import datetime
import rail
from guesttekinteractive.talent_user_import.mappers.user_sync_mapper import get_mapper_settings, is_valid_mapper_key
from guesttekinteractive.talent_user_import.config import INVALID_DATE, USER_STATUS_ENABLED, USER_STATUS_DISABLED


def categorize_users(talent_users, replicon_users_map):
    """
    Categorize users into new, update, disable, and skip categories.
    
    Args:
        talent_users (list): Delta users from Talent API
        replicon_users_map (dict): Employee ID to Replicon user mapping
        
    Returns:
        dict: Categorized users with 'new', 'update', 'disable', 'skip' keys
    """
    categorized = {
        'new': [],
        'update': [],
        'disable': [],
        'skip': []
    }
    
    for user in talent_users:
        emp_id = user.get('user_employee_id', '')
        location = user.get('user_location_name', '')
        department = user.get('org_level_code', '')
        
        # Validate mapper lookup
        if not is_valid_mapper_key(location, department):
            categorized['skip'].append({
                'user': user,
                'reason': f'Location/Department not in mapper: {location}/{department}'
            })
            continue
        
        # Check if user exists in Replicon
        if emp_id in replicon_users_map:
            replicon_user = replicon_users_map[emp_id]
            
            # Check if user is deactivated in Talent
            if user.get('user_deactivated', 0) == USER_STATUS_DISABLED:
                categorized['disable'].append({
                    'talent_user': user,
                    'replicon_user': replicon_user
                })
            else:
                categorized['update'].append({
                    'talent_user': user,
                    'replicon_user': replicon_user
                })
        else:
            # New user
            if user.get('user_deactivated', 0) == USER_STATUS_DISABLED:
                # Send to disable flow - process_users will do real-time lookup
                categorized['disable'].append({
                    'talent_user': user,
                    'replicon_user': None
                })
            else:
                categorized['new'].append(user)
    
    return categorized


def validate_mapper_lookup(location_name, department_code):
    """
    Validate that a location/department combination exists in the mapper.
    
    Args:
        location_name (str): Location name from Talent
        department_code (str): Department code from Talent
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not location_name:
        return False, "Location name is empty"
    
    if not department_code:
        return False, "Department code is empty"
    
    if is_valid_mapper_key(location_name, department_code):
        return True, None
    
    return False, f"Location/Department combination not found in mapper: {location_name}/{department_code}"


def is_valid_date(date_str):
    """
    Check if a date string is valid (not null, empty, or 0000-00-00).
    
    Args:
        date_str (str): Date string to validate
        
    Returns:
        bool: True if date is valid
    """
    if not date_str:
        return False
    
    if date_str == INVALID_DATE:
        return False
    
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def get_user_status_text(user_deactivated):
    """
    Get user status text from Talent deactivated flag.
    
    Args:
        user_deactivated (int): 0=Enabled, 1=Disabled
        
    Returns:
        str: 'Enabled' or 'Disabled'
    """
    return 'Disabled' if user_deactivated == USER_STATUS_DISABLED else 'Enabled'


def prepare_new_user_conf(talent_user, config, prereqs):
    """
    Prepare DAG configuration for processing a new user.
    
    Args:
        talent_user (dict): User data from Talent API
        config: Instance configuration
        prereqs (dict): Prerequisites data from Replicon
        
    Returns:
        dict: Configuration for child DAG
    """
    location_name = talent_user.get('user_location_name', '')
    department_code = talent_user.get('org_level_code', '')
    settings = get_mapper_settings(location_name, department_code)
    
    return {
        'employee_id': talent_user.get('user_employee_id', ''),
        'login_name': talent_user.get('user_email', ''),
        'first_name': talent_user.get('user_firstname', ''),
        'last_name': talent_user.get('user_lastname', ''),
        'email': talent_user.get('user_email', ''),
        'start_date': talent_user.get('user_start_date', ''),
        'end_date': talent_user.get('user_separation_date', ''),
        'user_deactivated': talent_user.get('user_deactivated', 0),
        'location_name': location_name,
        'department_code': department_code,
        'supervisor_employee_id': talent_user.get('user_supervisor_employee_id', ''),
        'talent_user_id': talent_user.get('user_id', ''),
        'mapper_settings': settings,
        # Include prereqs references
        'replicon_locations': rail.write_json_artifact(prereqs.get('locations', [])),
        'replicon_departments': rail.write_json_artifact(prereqs.get('departments', [])),
        'replicon_employeetypes': rail.write_json_artifact(prereqs.get('employeetypes', [])),
        'replicon_timesheet_templates': prereqs.get('timesheet_templates', []),
        'replicon_holiday_calendars': prereqs.get('holiday_calendars', []),
        'replicon_schedules': prereqs.get('schedules', []),
        'replicon_payrules': prereqs.get('payrules', []),
        'replicon_time_off_types': prereqs.get('time_off_types', []),
        'replicon_timezones': prereqs.get('timezones', []),
        'user_hire_date': talent_user.get('user_hire_date', ''),
        'employee_work_schedule_value': talent_user.get('employee_work_schedule_value', ''),
        'job_title': talent_user.get('job_title', ''),
        'job_type': talent_user.get('job_type', ''),
        'location_uri': find_location_uri(location_name, prereqs.get('locations', [])),
        'department_uri': find_department_uri(department_code, prereqs.get('department_groups', [])),
    }


def prepare_update_user_conf(talent_user, replicon_user, config, prereqs):
    """
    Prepare DAG configuration for updating an existing user.
    
    Args:
        talent_user (dict): User data from Talent API
        replicon_user (dict): User data from Replicon
        config: Instance configuration
        prereqs (dict): Prerequisites data
        
    Returns:
        dict: Configuration for child DAG
    """
    user_details = replicon_user.get('userDetails', {})
    
    return {
        'employee_id': talent_user.get('user_employee_id', ''),
        'useruri': user_details.get('uri', ''),
        'login_name': user_details.get('loginName', ''),
        'first_name': talent_user.get('user_firstname', ''),
        'last_name': talent_user.get('user_lastname', ''),
        'email': talent_user.get('user_email', ''),
        'start_date': talent_user.get('user_start_date', ''),
        'end_date': talent_user.get('user_separation_date', ''),
        'user_deactivated': talent_user.get('user_deactivated', 0),
        'location_name': talent_user.get('user_location_name', ''),
        'department_code': talent_user.get('org_level_code', ''),
        'supervisor_employee_id': talent_user.get('user_supervisor_employee_id', ''),
        'talent_user_id': talent_user.get('user_id', ''),
        'replicon_user_data': rail.write_json_artifact([replicon_user]),
        'user_hire_date': talent_user.get('user_hire_date', ''),
        'employee_work_schedule_value': talent_user.get('employee_work_schedule_value', ''),
        'job_title': talent_user.get('job_title', ''),
        'job_type': talent_user.get('job_type', ''),
        'location_uri': find_location_uri(talent_user.get('user_location_name', ''), prereqs.get('locations', [])),
        'department_uri': find_department_uri(talent_user.get('org_level_code', ''), prereqs.get('department_groups', [])),
    }


def can_user_profile_enable(dag_run):
    """
    Check if a user profile can be enabled.
    
    Args:
        dag_run: DAG run context
        
    Returns:
        bool: True if user is currently disabled and should be enabled
    """
    user_data = rail.result('get_user_data')
    if not user_data or not user_data[0]:
        return False
    
    is_login_enabled = user_data[0].get('userDetails', {}).get('isEnabled', True)
    talent_deactivated = dag_run.conf.get('user_deactivated', 0)
    
    # Enable if currently disabled in Replicon but active in Talent
    return not is_login_enabled and talent_deactivated == USER_STATUS_ENABLED


def get_effective_division_or_user(schedule, field_name):
    """
    Get effective value from a schedule (e.g., division or employeeTypeGroup).
    
    Args:
        schedule (list): Schedule data
        field_name (str): Field to extract ('division' or 'employeeTypeGroup')
        
    Returns:
        dict: Effective value or None
    """
    if not schedule:
        return None
    
    today = pendulum.now()
    
    for entry in schedule:
        date_range = entry.get('dateRange', {})
        start = date_range.get('startDate')
        end = date_range.get('endDate')
        
        if start:
            start_date = pendulum.datetime(start['year'], start['month'], start['day'])
        else:
            start_date = pendulum.datetime(1900, 1, 1)
        
        if end:
            end_date = pendulum.datetime(end['year'], end['month'], end['day'])
        else:
            end_date = pendulum.datetime(2999, 12, 31)
        
        if start_date <= today <= end_date:
            return entry.get(field_name)
    
    return None


def format_logs_for_csv(logs):
    """
    Format log entries for CSV output.
    
    Args:
        logs (list): List of log entries
        
    Returns:
        list: Formatted log entries for CSV
    """
    formatted = []
    
    for log in logs:
        formatted.append({
            'Employee ID': log.get('employee_id', ''),
            'Login Name': log.get('login_name', ''),
            'Name': f"{log.get('first_name', '')} {log.get('last_name', '')}",
            'Action': log.get('action', ''),
            'Status': log.get('status', ''),
            'Message': log.get('message', ''),
            'Timestamp': log.get('timestamp', pendulum.now().to_iso8601_string()),
        })
    
    return formatted


def do_format_logs(dag_run):
    """Format and consolidate all logs from the DAG run."""
    log_artifacts = []
    log_records = []

    for key in ['userlogs', 'otherlogs']:
        logs = dag_run.conf.get(key, '')
        if logs:
            if isinstance(logs, list):
                log_artifacts.extend(logs)
            else:
                log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = list(map(lambda log: {
        **{"ecid": log['ecid']},
        **dict(log['properties'].items()),
    }, log_records))

    error_count = len(list(filter(lambda x: x.get('status') == 'Error', final_log_records)))
    success_count = len(list(filter(lambda x: x.get('status') == 'Success', final_log_records)))
    exception_count = len(list(filter(lambda x: x.get('status') == 'Exception', final_log_records)))
    skipped_count = len(list(filter(lambda x: x.get('status') == 'Skipped', final_log_records)))

    rail.set_result(key="error_record_count", val=error_count)
    rail.set_result(key="success_record_count", val=success_count)
    rail.set_result(key="exception_record_count", val=exception_count)
    rail.set_result(key="skipped_record_count", val=skipped_count)
    rail.set_result(key="total_record_count", val=error_count + success_count + exception_count + skipped_count)

    return final_log_records


def get_task_state(task_id):
    """
    Get the state of a task for error handling.
    
    Args:
        task_id (str): Task ID to check
        
    Returns:
        str: Task state
    """
    try:
        return rail.get_task_state(task_id)
    except Exception:
        return 'unknown'


def should_skip_user_update(replicon_user):
    """
    Check if user update should be skipped due to 'Manually Updated' OEF.
    
    Args:
        replicon_user (dict): Replicon user data
        
    Returns:
        bool: True if update should be skipped
    """
    if not replicon_user or 'userDetails' not in replicon_user:
        return False
    
    custom_fields = replicon_user['userDetails'].get('customFieldValues', [])
    
    for field in custom_fields:
        field_def = field.get('customField', {})
        if field_def.get('displayText', '').lower() == 'manually updated':
            dropdown = field.get('dropDownOption', {})
            if dropdown and dropdown.get('displayText', '').lower() == 'yes':
                return True
    
    return False


def get_permission_set_details():
    """
    Get permission set details for user assignment.
    
    Returns:
        list: Permission set URIs to assign
    """
    permission_sets = rail.result('get_permission_sets')
    if not permission_sets:
        return []
    
    employee_perm = rail.find_first_by_attr_and_get_attr(
        permission_sets, 'displayText', 'Employee', 'uri'
    )
    
    if employee_perm:
        return [{'name': 'Employee', 'uri': employee_perm}]
    
    return []


def find_location_uri(location_name, locations):
    """
    Find location URI by name.
    
    Args:
        location_name (str): Location name to find
        locations (list): List of location data
        
    Returns:
        str: Location URI or None
    """
    if not location_name or not locations:
        return None
    
    location_lower = location_name.lower().strip()
    
    for loc in locations:
        if loc.get('name', '').lower().strip() == location_lower:
            return loc.get('uri')
        if loc.get('fullpath', '').lower().strip() == location_lower:
            return loc.get('uri')
    
    return None


def find_department_uri(department_code, departments):
    if not department_code or not departments:
        return None
    
    code_lower = department_code.lower().strip()
    
    for dept in departments:
        display_text = dept.get('displayText', '')
        # Match code at the start: "5030 - Inside Sales" starts with "5030"
        if display_text.lower().startswith(code_lower):
            return dept.get('uri')
    
    return None


def find_employeetype_uri(usertype_name, employeetypes):
    """
    Find employee type group URI by name.
    
    Args:
        usertype_name (str): User type name to find
        employeetypes (list): List of employee type data
        
    Returns:
        str: Employee type URI or None
    """
    if not usertype_name or not employeetypes:
        return None
    
    type_lower = usertype_name.lower().strip()
    
    for et in employeetypes:
        if et.get('name', '').lower().strip() == type_lower:
            return et.get('uri')
        if et.get('fullpath', '').lower().strip() == type_lower:
            return et.get('uri')
    
    return None
