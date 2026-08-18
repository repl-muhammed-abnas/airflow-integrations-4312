"""
Response Filters - GuestTek Talent User Import Integration

Provides functions to filter and transform API responses from both Talent and Replicon APIs.
These filters extract relevant data, format responses, and prepare data for downstream processing.

Key features:
    - Talent API user response filtering
    - Replicon entity data extraction
    - Delta detection filtering
    - User data normalization
    - Group membership parsing

Functions:
    filter_talent_users_response: Filter and normalize Talent API user data
    filter_delta_users: Filter users modified within delta window
    get_filtered_user_data: Extract user details from Replicon response
    filter_all_location_data: Filter location hierarchy data
    filter_all_employeetype_groups_data: Filter employee type groups
    get_effective_user_groupmembership_filter: Parse user group memberships
    filter_timesheet_period_list: Filter timesheet periods
    get_udf_uris: Extract custom field URIs
    get_required_permission: Extract permission set URIs
"""
from datetime import datetime, timedelta
import pendulum
import html

null = None


def _normalize_date(date_str):
    """
    Normalize date string to YYYY-MM-DD format.
    
    The individual user endpoint returns dates like '0000-00-00 00:00:00' with
    a time component, but downstream code (get_replicon_date, INVALID_DATE check)
    expects just '0000-00-00' (10 chars).
    
    Args:
        date_str (str): Date string, possibly with time component
        
    Returns:
        str: Date string truncated to YYYY-MM-DD, or empty string
    """
    if not date_str:
        return ''
    return date_str[:10] if len(date_str) >= 10 else date_str


def filter_talent_users_response(response):
    """
    Filter and normalize Talent API users response.
    
    Args:
        response (dict): Raw Talent API response from /api/v1/users
        
    Returns:
        list: List of normalized user dictionaries
    """
    if not response or 'data' not in response:
        return []
    
    users = []
    for user in response.get('data', []):
        # Talent API returns flat structure - fields directly on user object
        users.append({
            'user_id': user.get('user_id'),
            'user_employee_id': user.get('user_employee_id', ''),
            'user_email': user.get('user_email', ''),
            'user_firstname': html.unescape(user.get('user_firstname', '')),
            'user_lastname': html.unescape(user.get('user_lastname', '')),
            'user_start_date': _normalize_date(user.get('user_start_date', '')),
            'user_separation_date': _normalize_date(user.get('user_separation_date', '')),
            'user_termination_reason': user.get('user_termination_reason', ''),
            'user_deactivated': user.get('user_deactivated', 0),
            'user_location_name': user.get('user_location_name', ''),
            'org_level_code': user.get('org_level_code', ''),
            'user_last_modified': user.get('user_last_modified', ''),
            # Supervisor is stored as manager_employee_id in API
            'user_supervisor_employee_id': user.get('manager_employee_id', ''),
            # Employee Type source
            'employee_work_schedule_value': html.unescape(user.get('employee_work_schedule_value', '')),
            # Role source
            'job_title': html.unescape(user.get('job_title', '')),
            # Service Center source
            'job_type': html.unescape(user.get('job_type', '')),
            # Effective date for group assignments
            'employee_effective_date': user.get('employee_effective_date', ''),
            'user_hire_date': _normalize_date(user.get('user_hire_date', '')),
        })
    
    return users


def filter_event_logs_response(response):
    """
    Extract unique user_ids from Talent API event-logs response.
    
    Args:
        response (dict): Raw Talent API response from /api/v1/event-logs
        
    Returns:
        set: Set of unique user_id integers
    """
    if not response or 'data' not in response:
        return set()
    
    user_ids = set()
    for event in response.get('data', []):
        user_id = event.get('user_id')
        if user_id:
            user_ids.add(user_id)
    
    return user_ids


def filter_delta_users(users, delta_hours=24):
    """
    Filter users modified within the delta window.
    
    Args:
        users (list): List of user dictionaries
        delta_hours (int): Hours to look back for modifications
        
    Returns:
        list: Users modified within the delta window
    """
    if not users:
        return []
    
    cutoff_time = pendulum.now('UTC').subtract(hours=delta_hours)
    delta_users = []
    
    for user in users:
        last_modified = user.get('user_last_modified', '')
        if not last_modified:
            continue
        
        try:
            # Parse the last modified timestamp
            if isinstance(last_modified, str):
                modified_dt = pendulum.parse(last_modified)
            else:
                modified_dt = pendulum.instance(last_modified)
            
            if modified_dt >= cutoff_time:
                delta_users.append(user)
        except Exception:
            # If we can't parse the date, include the user to be safe
            delta_users.append(user)
    
    return delta_users


def get_filtered_user_data(response):
    """
    Extract and filter user data from Replicon BulkGetUsers3 response.
    
    Args:
        response (dict): Raw Replicon API response
        
    Returns:
        list: List of user data dictionaries
    """
    if not response:
        return []
    
    users = response if isinstance(response, list) else [response]
    filtered = []
    
    for user in users:
        if user and user.get('userDetails'):
            filtered.append(user)
    
    return filtered


def filter_all_location_data(all_results):
    locations = []
    for result in all_results:
        for row in result.get('rows', []):
            cells = row.get('cells', [])
            if len(cells) >= 1:
                locations.append({
                    'uri': cells[0].get('uri', ''),
                    'name': cells[0].get('textValue', cells[0].get('displayText', '')),
                    'slug': cells[0].get('slug', ''),
                })
    return locations


def filter_all_employeetype_groups_data(all_results):
    """
    Filter and normalize employee type group data from Replicon.
    
    Args:
        all_results (list): Paginated results from EmployeeTypeGroupListService
        
    Returns:
        list: Normalized employee type group data
    """
    groups = []
    
    for result in all_results:
        for row in result.get('rows', []):
            cells = row.get('cells', [])
            if len(cells) >= 2:
                groups.append({
                    'uri': cells[0].get('uri', ''),
                    'name': cells[0].get('displayText', ''),
                    'fullpath': cells[1].get('displayText', ''),
                    'slug': cells[0].get('slug', ''),
                })
    
    return groups


def filter_all_division_data(all_results):
    """
    Filter and normalize division (department) data from Replicon.
    
    Args:
        all_results (list): Paginated results from DivisionListService
        
    Returns:
        list: Normalized division data
    """
    divisions = []
    
    for result in all_results:
        for row in result.get('rows', []):
            cells = row.get('cells', [])
            if len(cells) >= 2:
                divisions.append({
                    'uri': cells[0].get('uri', ''),
                    'name': cells[0].get('displayText', ''),
                    'fullpath': cells[1].get('displayText', ''),
                    'slug': cells[0].get('slug', ''),
                })
    
    return divisions


def get_effective_user_groupmembership_filter(response):
    """
    Parse user group membership response to extract current groups.
    
    Args:
        response (dict): GetEffectiveUserGroupMembership response
        
    Returns:
        dict: Parsed group membership data
    """
    if not response:
        return {'groups': []}
    
    groups = []
    memberships = response if isinstance(response, list) else [response]
    
    for membership in memberships:
        if membership.get('userGroup'):
            groups.append({
                'uri': membership['userGroup'].get('uri', ''),
                'name': membership['userGroup'].get('displayText', ''),
            })
    
    return {'groups': groups}


def filter_timesheet_period_list(all_results):
    """
    Filter timesheet period list data.
    
    Args:
        all_results (list): Paginated results from TimesheetPeriodListService
        
    Returns:
        list: Filtered timesheet period data
    """
    periods = []
    
    for result in all_results:
        for row in result.get('rows', []):
            cells = row.get('cells', [])
            if len(cells) >= 2:
                periods.append({
                    'uri': cells[0].get('uri', ''),
                    'name': cells[1].get('displayText', ''),
                })
    
    return periods


def get_udf_uris(response, custom_fields):
    """
    Extract custom field (UDF) URIs from GetAllCustomFields response.
    
    Args:
        response (list): GetAllCustomFields response
        custom_fields (list): List of custom field names to find
        
    Returns:
        dict: Mapping of field names to URIs
    """
    udf_map = {}
    
    if not response:
        return udf_map
    
    for field in response:
        field_name = field.get('displayText', '')
        if field_name in custom_fields:
            udf_map[field_name.lower().replace(' ', '_') + '_uri'] = field.get('uri', '')
    
    return udf_map


def get_required_permission(response, config):
    """
    Extract required permission set URIs.
    
    Args:
        response (list): GetAllPermissionSets response
        config: Configuration object with permission list
        
    Returns:
        list: Permission set data with URIs
    """
    permissions = []
    
    if not response:
        return permissions
    
    for perm in response:
        permissions.append({
            'uri': perm.get('uri', ''),
            'name': perm.get('displayText', ''),
        })
    
    return permissions


def filter_talent_additional_info(response):
    """
    Filter additional user info from Talent API.
    
    Args:
        response (dict): Response from /api/v1/employee-additional-information/{id}
        
    Returns:
        dict: Extracted additional info (LOA, Date of LOA)
    """
    if not response or 'data' not in response:
        return {}
    
    data = response.get('data', [])
    if not data:
        return {}
    
    result = {}
    section_a = data[0].get('section_a', [])
    
    for field in section_a:
        display_name = field.get('field_name_display', '')
        value = field.get('field_answer_value', '')
        
        if display_name == 'Leave Of Absence':
            result['leave_of_absence'] = value
        elif display_name == 'Date of LOA':
            result['date_of_loa'] = value

        elif display_name == 'Preferred Name':
            result['preferred_name'] = value
    
    return result


def filter_replicon_users_for_comparison(response):
    """
    Filter Replicon users response for comparison with Talent data.
    
    Args:
        response (list): BulkGetUsers3 response
        
    Returns:
        dict: Employee ID to user data mapping
    """
    user_map = {}
    
    if not response:
        return user_map
    
    for user in response:
        if user and user.get('userDetails'):
            emp_id = user['userDetails'].get('employeeId', '')
            if emp_id:
                user_map[emp_id] = user
    
    return user_map


def get_manually_updated_value(user_data):
    """
    Check if user has 'Manually Updated' OEF set to 'Yes'.
    
    Args:
        user_data (dict): User data from Replicon
        
    Returns:
        bool: True if manually updated flag is set
    """
    if not user_data or 'userDetails' not in user_data:
        return False
    
    custom_fields = user_data['userDetails'].get('customFieldValues', [])
    
    for field in custom_fields:
        field_def = field.get('customField', {})
        if field_def.get('displayText', '').lower() == 'manually updated':
            dropdown = field.get('dropDownOption', {})
            if dropdown and dropdown.get('displayText', '').lower() == 'yes':
                return True
    
    return False
