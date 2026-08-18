import itertools
from typing import Dict, Any, List

null = None

def get_timesheet_details(response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
    res = []
    for ts in flatten_rows:
        res.append({
        "timesheet_status": timesheet_status_mapping.get(ts['statusUri'].split(':')[-1], ''),
        "timesheet_status_uri": ts['statusUri'],
        "timesheet_uri": ts['uri'],
        "timesheet_date_range": ts['dateRange'],
        "user_uri": ts['owner']['uri']
    })
    return list(map(lambda ts: {
        "timesheet_status": timesheet_status_mapping.get(ts['statusUri'].split(':')[-1], ''),
        "timesheet_status_uri": ts['statusUri'],
        "timesheet_uri": ts['uri'],
        "timesheet_date_range": ts['dateRange'],
        "user_uri": ts['owner']['uri']
    }, flatten_rows))

def filter_all_tags_details(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not response['tags']:
        return []
    return list(filter(lambda item: item['isnabled'] == True, map(lambda row: {
        "name": row['name'],
        "uri": row['uri'],
        "isnabled": row['isEnabled'],
    }, response['tags'])))

def filter_time_entries(response: List[List[Dict[str, Any]]]) -> List[str]:
    if not response:
        return []
    flatten_rows = list(itertools.chain(
        map(lambda x: x, response)))
    uris = set()
    for entry in flatten_rows:
        for item in entry:
            uri = item.get('uri')
            if uri:
                uris.add(uri)
    return list(uris)

def filter_punch_entries(response):
    if not response:
        return []
    flatten_rows = list(itertools.chain(
        map(lambda x: x, response)))
    from collections import OrderedDict
    uris = OrderedDict()
    for entry in flatten_rows:
        for entry in entry[0]["timePunches"]:
            uris[entry["uri"]] = 1
    return list(uris.keys())

def format_project_task_details(response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tasks = []

    def add_child_tasks(child_tasks: List[Dict[str, Any]], parent: str) -> None:
        for child_task in child_tasks:
            current_task = f"{parent}|{child_task['task']['name']}"
            child_task['task']['full_task_name'] = current_task
            tasks.append(child_task['task'])
            if child_task['childTasks']:
                add_child_tasks(child_task['childTasks'], current_task)

    for item in response:
        root_task_name = item['task']['name']
        item['task']['full_task_name'] = root_task_name
        tasks.append(item['task'])
        if item.get('childTasks'):
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
            item['timeEntryDateRange']['startDate'] else None,
        "enddate": str(item['timeEntryDateRange']['endDate']['month']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['day']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['year']) if item['timeEntryDateRange'] and \
            item['timeEntryDateRange']['endDate'] else None
    }, tasks))

    return tasks


