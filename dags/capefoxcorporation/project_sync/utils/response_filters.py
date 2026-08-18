"""
Response filter functions for CostPoint project sync.
Contains functions that process and filter API responses.
"""
import itertools
from capefoxcorporation.project_sync.utils import custom_methods
from capefoxcorporation.project_sync.utils.custom_methods import map_workforce_empid
import rail


def filter_costpoint_projects_response(data):
    """Filter and process CostPoint projects API response"""
    return data['document']['rows']


def filter_workforce_user_costpoint_response(data):
    """Filter and process CostPoint workforce users API response"""
    return data['document']['rows']


def filter_project_details_response(data):
    """Filter and process Replicon project details API response"""
    return None if data['errors'] else data['results'][0]


def filter_project_leader_response(data):
    """Filter and process project leader API response"""
    return data[0]


def do_user_data_handler(data):
    """Process user data from Replicon API response"""
    emp_ids = map_workforce_empid()
    return list(map(lambda x: {"employeeId": x, 'userDetails': data[emp_ids.index(x)]}, emp_ids))



def do_get_task_info_from_replicon(dag_run):
    """Extract task information from Replicon project details response"""
    tasks = []
    if rail.result('get_project_details'):
        cp_data = custom_methods.get_project_data(dag_run)[1]
        # Use root project URI as initial parent
        root_project_uri = rail.result('get_project_details').get('project', {}).get('uri')
        get_task_data_replicon(rail.result('get_project_details')[
            'tasks'], tasks, cp_data, root_project_uri)
    return tasks


def get_task_data_replicon(tasks, result, cp_data, parent_uri=None):
    """Recursively extract task data from Replicon response with parent URI tracking.

    Extracts both identification fields and preservable fields that should not be
    overwritten during sync (like percentCompleted, estimatedHours, etc.)
    """
    for task in tasks:
        task_data = task['task']
        result.append({
            # Identification fields
            'code': task_data['code'],
            'name': task_data['name'],
            'uri': task_data['uri'],
            'isClosed': task_data.get('isClosed', False),
            'parent_uri': parent_uri,
            'new_name': custom_methods.get_new_task_name(cp_data, task_data['code']),
            # Preservable fields - these should be maintained from Replicon during updates
            'percentCompleted': task_data.get('percentCompleted', '0'),
            'estimatedHours': task_data.get('estimatedHours'),
            'estimatedCost': task_data.get('estimatedCost'),
            'customFieldValues': task_data.get('customFieldValues', []),
            'extensionFieldValues': task_data.get('extensionFieldValues', []),
            'timeEntryDateRange': task_data.get('timeEntryDateRange'),
            'timeAndMaterials': task_data.get('timeAndMaterials'),
            'keyValues': task_data.get('keyValues', []),
            'historicalKeyValues': task_data.get('historicalKeyValues', []),
            'costTypeUri': task_data.get('costTypeUri'),
        })
        # Pass current task's URI as parent for its children
        get_task_data_replicon(task['childTasks'], result, cp_data, task_data['uri'])