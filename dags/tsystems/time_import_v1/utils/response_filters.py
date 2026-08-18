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
                add_child_tasks(child_task['childTasks'], child_task['task']['name'])

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
