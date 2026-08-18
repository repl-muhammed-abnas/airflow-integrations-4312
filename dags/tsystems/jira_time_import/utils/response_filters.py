"""Response filters for T-Systems Time Import API responses."""
import itertools
import rail

null = None

def get_timesheet_details(response):
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

def filter_time_entries(response, dag_run):
    """Filter time entries from API response - enhanced for T-Systems with unique ID matching"""
    if not response:
        return []

    entries = []

    for entry in response:
        task_uri = null
        project_uri = null
        activity_uri = null
        comments = null
        unique_id = null

        if 'customMetadata' in entry:
            for meta in entry.get('customMetadata', []):
                key_uri = meta.get('keyUri', {})
                if key_uri == "urn:replicon:time-entry-metadata-key:task":
                    task_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:project":
                    project_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:activity":
                    activity_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:comments":
                    comments = meta.get('value', {}).get('text')
                elif key_uri == "urn:replicon:time-entry-metadata-key:external-id":
                    unique_id = meta.get('value', {}).get('text')

        hours = 0
        if 'interval' in entry and entry['interval'] and 'hours' in entry['interval']:
            seconds = entry['interval']['hours'].get('seconds', 0)
            minutes = entry['interval']['hours'].get('minutes', 0)
            hrs = entry['interval']['hours'].get('hours', 0)
            hours = hrs + (minutes / 60) + (seconds / 3600)

        entries.append({
            'entry_uri': entry.get('uri'),
            'user_uri': entry.get('user', {}).get('uri'),
            'entry_date': entry.get('entryDate'),
            'total_hours': round(hours, 2),
            'task_uri': task_uri,
            'project_uri': project_uri,
            'activity_uri': activity_uri,
            'comments': comments,
            'unique_id': unique_id
        })

    if not entries:
        return []

    target_unique_id = dag_run.conf["input_data"]["unique_id"]

    for entry in entries:
        if entry.get('unique_id') == target_unique_id:
            return entry.get('entry_uri')
    return null


def format_project_task_details(response, dag_run):
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

    def add_child_tasks(child_tasks, parent_full_name):
        """
        Recursively process child tasks and build hierarchical task names.
        
        Args:
            child_tasks: List of child task objects to process
            parent_full_name: Full hierarchical parent task name for building hierarchy
        """
        for child_task in child_tasks:
            child_task['task']['full_task_name'] = f"{parent_full_name}|{child_task['task']['name']}"
            tasks.append(child_task['task'])
            if child_task['childTasks']:
                add_child_tasks(child_task['childTasks'], child_task['task']['full_task_name'])

    for item in response:
        item['task']['full_task_name'] = f"{item['task']['name']}"
        tasks.append(item['task'])
        add_child_tasks(item['childTasks'], item['task']['full_task_name'])

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
    }, filter(lambda item: item['full_task_name'] == dag_run.conf['input_data']['full_task_path'], tasks)))

    return tasks
