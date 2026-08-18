"""
Sand Tech Inc - User Import Data Handlers
Response handlers and filters for Replicon API responses
"""

import itertools


def page_handler(request, result):
    """
    Standard pagination handler for Replicon list services
    Continues pagination if results exist
    
    Args:
        request: Current request dict
        result: Response from previous page
    
    Returns:
        Modified request for next page, or None to stop
    """
    if len(result.get('rows', [])) > 0:
        request['page'] += 1
        return request
    return None


def user_search_result_handler(result, email):
    """
    Handler for user search results - finds user by email
    
    Args:
        result: List of paginated results
        email: Email to search for (case-insensitive)
    
    Returns:
        Dict with user info or empty dict if not found
    """
    flattened_rows = list(itertools.chain(*[x.get('rows', []) for x in result]))
    
    email_lower = email.lower() if email else ''
    
    for row in flattened_rows:
        cells = row.get('cells', [])
        if len(cells) >= 4:
            login_name = cells[1].get('textValue', '')
            if login_name and login_name.lower() == email_lower:
                return {
                    'username': cells[0].get('textValue', ''),
                    'loginname': login_name,
                    'employeeid': cells[2].get('textValue', ''),
                    'status': cells[3].get('textValue', ''),
                    'useruri': cells[1].get('uri', '')
                }
    
    return {}


def user_search_by_empid_handler(result, employee_id):
    """
    Handler for user search results - finds user by Employee ID
    
    Args:
        result: List of paginated results
        employee_id: Employee ID to search for
    
    Returns:
        Dict with user info or empty dict if not found
    """
    flattened_rows = list(itertools.chain(*[x.get('rows', []) for x in result]))
    
    empid_str = str(employee_id).strip() if employee_id else ''
    
    for row in flattened_rows:
        cells = row.get('cells', [])
        if len(cells) >= 4:
            row_empid = cells[2].get('textValue', '')
            if row_empid and str(row_empid).strip() == empid_str:
                return {
                    'username': cells[0].get('textValue', ''),
                    'loginname': cells[1].get('textValue', ''),
                    'employeeid': row_empid,
                    'status': cells[3].get('textValue', ''),
                    'useruri': cells[1].get('uri', '')
                }
    
    return {}


def get_filtered_departments(response):
    """
    Filter departments from DepartmentGroupListService response
    
    Args:
        response: Response object from API call
    
    Returns:
        List of dicts with name and uri
    """
    try:
        data = response.json().get('d', {}).get('rows', [])
        return [{
            'name': item['cells'][0].get('textValue', ''),
            'uri': item['cells'][0].get('uri', '')
        } for item in data if item.get('cells')]
    except (KeyError, IndexError, TypeError):
        return []


def get_filtered_timesheet_periods(response):
    """
    Filter timesheet periods from TimesheetPeriodListService response
    
    Args:
        response: Response object from API call
    
    Returns:
        List of dicts with name and uri
    """
    try:
        data = response.json().get('d', {}).get('rows', [])
        return [{
            'name': item['cells'][0].get('textValue', ''),
            'uri': item['cells'][0].get('uri', '')
        } for item in data if item.get('cells')]
    except (KeyError, IndexError, TypeError):
        return []


def get_bulk_users_handler(response):
    """
    Handler for BulkGetUsers3 response - extracts first user
    
    Args:
        response: List of user data from API
    
    Returns:
        First user dict or None
    """
    if response and len(response) > 0:
        return response[0]
    return None


def check_user_has_permission(user_data, permission_name):
    """
    Check if user has a specific permission
    
    Args:
        user_data: User data dict from BulkGetUsers3
        permission_name: Name of permission to check (e.g., "Supervisor")
    
    Returns:
        Permission URI if found, None otherwise
    """
    if not user_data:
        return None
    
    permission_sets = user_data.get('permissionSets', [])
    for perm in permission_sets:
        if perm.get('name', '').lower() == permission_name.lower():
            return perm.get('uri')
    
    return None


def extract_current_supervisor(supervisor_data):
    """
    Extract current supervisor info from GetSupervisorAssignmentDetails response
    
    Args:
        supervisor_data: Response from GetSupervisorAssignmentDetails
    
    Returns:
        Dict with supervisor info or None
    """
    if not supervisor_data or not supervisor_data.get('supervisor'):
        return None
    
    supervisor = supervisor_data['supervisor']
    user = supervisor.get('user', {})
    
    return {
        'uri': user.get('uri', ''),
        'loginName': user.get('loginName', ''),
        'displayText': user.get('displayText', '')
    }


def extract_current_department(group_membership):
    """
    Extract current department from GetEffectiveUserGroupMembership response
    
    Args:
        group_membership: Response from GetEffectiveUserGroupMembership
    
    Returns:
        Department URI or None
    """
    if not group_membership:
        return None
    
    departments = group_membership.get('departments', [])
    if departments and len(departments) > 0:
        dept = departments[0].get('department', {}).get('department', {})
        return dept.get('uri')
    
    return None


def extract_current_location(group_membership):
    """
    Extract current location from GetEffectiveUserGroupMembership response
    
    Args:
        group_membership: Response from GetEffectiveUserGroupMembership
    
    Returns:
        Location URI or None
    """
    if not group_membership:
        return None
    
    locations = group_membership.get('locations', [])
    if locations and len(locations) > 0:
        loc = locations[0].get('location', {}).get('location', {})
        return loc.get('uri')
    
    return None


def extract_holiday_calendar_uri(user_data):
    """
    Extract holiday calendar URI from user data
    
    Args:
        user_data: User data dict from BulkGetUsers3
    
    Returns:
        Holiday calendar URI or None
    """
    if not user_data:
        return None
    
    calendar = user_data.get('holidayCalendar')
    if calendar:
        return calendar.get('uri')
    
    return None


def format_report_rows_as_users(report_data):
    """
    Format report data rows as user dicts
    Expects report with columns: User Name, Login Name, Employee ID, UserUri, User Status
    
    Args:
        report_data: List of rows from report CSV
    
    Returns:
        List of user dicts
    """
    users = []
    for row in report_data:
        users.append({
            'username': row.get('User Name', ''),
            'loginname': row.get('Login Name', ''),
            'employeeid': row.get('Employee ID', ''),
            'useruri': row.get('UserUri', ''),
            'status': row.get('User Status', '')
        })
    return users
