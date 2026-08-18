"""
Response Filters and Data Handlers for iPipeline JIRA-Replicon Integration
Processes and filters API responses for downstream use
"""

import pendulum
import itertools
import rail

null = None


def get_timesheet_details_for_user_and_date(response):
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

    return {
        "timesheet_status": response['timesheet']['statusUri'].split(':')[-1],
        "timesheet_status_uri": response['timesheet']['statusUri'],
        "timesheet_uri": response['timesheet']['uri'],
        "timesheet_date_range": response['timesheet']['dateRange']
    }


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
                add_child_tasks(
                    child_task['childTasks'], child_task['task']['full_task_name'])

    for item in response:
        item['task']['full_task_name'] = f"{item['task']['name']}"
        tasks.append(item['task'])
        add_child_tasks(item['childTasks'], item['task']['full_task_name'])

    tasks_list_final = list(map(lambda item: {
        "task_name": item['name'],
        "task_code": item['code'],
        "uri": item['uri'],
        "full_task_name": item['full_task_name'],
        'isclosed': item['isClosed'],
        "startdate": str(item['timeEntryDateRange']['startDate']['month']) + '/' + str(
            item['timeEntryDateRange']['startDate']['day']) + '/' + str(
                item['timeEntryDateRange']['startDate']['year']) if item['timeEntryDateRange'] and item['timeEntryDateRange']['startDate'] else null,
        "enddate": str(item['timeEntryDateRange']['endDate']['month']) + '/' + str(
            item['timeEntryDateRange']['endDate']['day']) + '/' + str(
                item['timeEntryDateRange']['endDate']['year']) if item['timeEntryDateRange'] and item['timeEntryDateRange']['endDate'] else null
    }, filter(lambda item: item['full_task_name'] == dag_run.conf['task_type'], tasks)))

    return tasks_list_final


def filter_time_entries(response, dag_run):
    """Filter time entries from API response"""
    if not response:
        return []

    entries = []

    for entry in response:
        task_uri = null
        project_uri = null
        comments = null

        if 'customMetadata' in entry:
            for meta in entry.get('customMetadata', []):
                key_uri = meta.get('keyUri', {})
                if key_uri == "urn:replicon:time-entry-metadata-key:task":
                    task_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:project":
                    project_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:comments":
                    comments = meta.get('value', {}).get('text')

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
            'comments': comments,
        })

    if not entries:
        return []

    target_task_uri = rail.find_first_by_attr_and_get_attr(rail.result(
        'get_required_tasks_for_project'), 'full_task_name', dag_run.conf['task_type'], 'uri', '')

    for entry in entries:
        if entry.get('task_uri') == target_task_uri:
            return entry.get('entry_uri')
    return null
