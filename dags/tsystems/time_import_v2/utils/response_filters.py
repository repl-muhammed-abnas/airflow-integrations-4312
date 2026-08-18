"""Response filters for T-Systems Time Import API responses."""
import itertools
from typing import Dict, Any, List, Optional

null = None

def get_timesheet_details(response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract and format timesheet details from Replicon API response.
    
    Processes GetTimesheetDetailsForDate API response to extract timesheet
    status, URI, date range, and user information for timesheet management.
    
    Args:
        response: List of API response objects containing timesheet data
        
    Returns:
        List[Dict[str, Any]]: Formatted timesheet details with status mappings
    """
    if not response:
        return []

    timesheet_status_mapping = {
        'waiting': 'Waiting for Approval',
        'open': 'Not Submitted',
        'rejected': 'Rejected',
        'approved': 'Approved'
    }
    flatten_rows = list(itertools.chain(
        list(map(lambda x: x['timesheet'], response))))
    return list(map(lambda ts: {
        "timesheet_status": timesheet_status_mapping.get(ts['statusUri'].split(':')[-1], ''),
        "timesheet_status_uri": ts['statusUri'],
        "timesheet_uri": ts['uri'],
        "timesheet_date_range": ts['dateRange'],
        "user_uri": ts['owner']['uri']
    }, flatten_rows))

def filter_all_tags_details(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Filter and format enabled OEF tags from Replicon API response.
    
    Processes GetObjectExtensionTagDefinitionDetails API response to extract
    only enabled tags for Object Extension Field configuration.
    
    Args:
        response: API response dictionary containing tags array
        
    Returns:
        List[Dict[str, Any]]: List of enabled tags with name, URI, and status
    """
    if not response['tags']:
        return []
    return list(filter(lambda item: item['isnabled'] == True, map(lambda row: {
        "name": row['name'],
        "uri": row['uri'],
        "isnabled": row['isEnabled'],
    }, response['tags'])))

def filter_time_entries(response: List[List[Dict[str, Any]]]) -> List[str]:
    """
    Extract unique time entry URIs from GetTimeEntryRevisionGroups API response.
    
    Processes API response to extract time entry revision group URIs that
    need to be deleted before adding new time entries.
    
    Args:
        response: Nested list structure from time entry API response
        
    Returns:
        List[str]: List of unique time entry revision group URIs for deletion
    """
    if not response:
        return []
    flatten_rows = list(itertools.chain(
        map(lambda x: x, response)))
    uris = set()
    for entry in flatten_rows:
        for item in entry:
            uris.add(item['uri'])
    return list(uris)


def format_project_task_details(response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format project task hierarchy from GetDescendantTaskDetails API response.
    
    Processes task hierarchy response to create flat list of tasks with
    full hierarchical names, date ranges, and status information.
    
    Args:
        response: List of task hierarchy objects from Replicon API
        
    Returns:
        List[Dict[str, Any]]: Flattened list of tasks with formatted details
    """
    tasks = []

    def add_child_tasks(child_tasks: List[Dict[str, Any]], parent: str) -> None:
        """
        Recursively process child tasks and build hierarchical task names.
        
        Args:
            child_tasks: List of child task objects to process
            parent: Parent task name for building hierarchy
        """
        for child_task in child_tasks:
            child_task['task']['full_task_name'] = f"{parent}|{child_task['task']['name']}"
            tasks.append(child_task['task'])
            if child_task['childTasks']:
                add_child_tasks(child_task['childTasks'], child_task['task']['full_task_name'])

    for item in response:
        item['task']['full_task_name'] = f"{item['task']['name']}"
        tasks.append(item['task'])
        add_child_tasks(item['childTasks'], item['task']['name'])

    tasks = list(map(lambda item: {
        "task_name": item['name'],
        "task_code": item['code'],
        "uri": item['uri'],
        "full_task_name": item['full_task_name'],
        'isclosed': item['isClosed'],
        "startdate": str(item['timeEntryDateRange']['startDate']['month']) + '/' + 
        str(item['timeEntryDateRange']['startDate']['day']) + '/' + 
        str(item['timeEntryDateRange']['startDate']['year']) if item['timeEntryDateRange'] and \
            item['timeEntryDateRange']['startDate'] else null,
        "enddate": str(item['timeEntryDateRange']['endDate']['month']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['day']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['year']) if item['timeEntryDateRange'] and \
            item['timeEntryDateRange']['endDate'] else null
    }, tasks))

    return tasks


def build_employee_username_map(response: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Build an employeeId -> username lookup map from a bulk BulkGetUsers3 response.

    Username resolution prefers customDisplayName, falling back to displayText,
    since Replicon returns customDisplayName as an explicit null for some users.

    Args:
        response: List of {"userDetails": {...}} items from BulkGetUsers3

    Returns:
        Dict[str, str]: Mapping of employeeId to resolved username (blank if unresolved)
    """
    if not response:
        return {}

    username_map = {}
    for item in response:
        user_details = item.get('userDetails') if item else None
        employee_id = user_details.get('employeeId') if user_details else None
        if not employee_id:
            continue
        username_map[employee_id] = user_details.get('customDisplayName') or user_details.get('displayText') or ''
    return username_map


def filter_user_group_assignments(response):
    """
    Extract user's current group membership from GetEffectiveUserGroupMembership API response.

    Processes API response to extract department, service center, location (org structure),
    and employee type assignments for validating project resource access.

    Args:
        response: API response dictionary containing group membership arrays

    Returns:
        Dict[str, Any]: Dictionary containing URIs and names for each group type
    """
    def safe_get_nested_value(array_key, item_key, field):
        """Safely extract nested values from group membership arrays"""
        if not response:
            return null

        array = response.get(array_key)
        if not array:
            return null

        first_item = array[0] if array and len(array) > 0 else null
        if not first_item:
            return null

        nested_item = first_item.get(item_key) if first_item else null
        if not nested_item:
            return null

        final_item = nested_item.get(item_key) if nested_item else null
        if not final_item:
            return null

        return final_item.get(field) if final_item else null

    return {
        "location_uri": safe_get_nested_value('locations', 'location', 'uri'),
        "location_name": safe_get_nested_value('locations', 'location', 'displayText'),
        "department_uri": safe_get_nested_value('departments', 'department', 'uri'),
        "department_name": safe_get_nested_value('departments', 'department', 'displayText'),
        "service_center_uri": safe_get_nested_value('serviceCenters', 'serviceCenter', 'uri'),
        "service_center_name": safe_get_nested_value('serviceCenters', 'serviceCenter', 'displayText'),
        "employee_type_uri": safe_get_nested_value('employeeTypes', 'employeeType', 'uri'),
        "employee_type_name": safe_get_nested_value('employeeTypes', 'employeeType', 'displayText')
    }
