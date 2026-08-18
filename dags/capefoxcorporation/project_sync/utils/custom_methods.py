"""
Custom helper methods for CostPoint project sync.
Contains utility functions and business logic helpers.
"""
from datetime import datetime
from pytz import timezone
import pendulum
from airflow.models import Variable
import itertools
import rail


# ============================================================================
# Common Structure Builders (to avoid repetition)
# ============================================================================

def _build_assigned_resource_dict(user_uri):
    """Build a single assigned resource dictionary with complete API structure."""
    return {
        "uri": None,
        "resourcePlaceholderParameterCorrelationId": None,
        "user": {
            "uri": user_uri,
            "loginName": None,
            "employeeId": None,
            "parameterCorrelationId": None
        },
        "department": None,
        "placeholder": None,
        "location": None,
        "division": None,
        "costCenter": None,
        "serviceCenter": None,
        "departmentGroup": None,
        "employeeTypeGroup": None
    }


def _extract_task_fields(task):
    """Extract all preservable fields from a task (identification + preservable).

    Used by both get_existing_children_for_parent() and get_existing_task_by_code().
    """
    return {
        # Identification fields
        'code': task['code'],
        'name': task['name'],
        'uri': task['uri'],
        'isClosed': task.get('isClosed', False),
        # Preservable fields
        'percentCompleted': task.get('percentCompleted', '0'),
        'estimatedHours': task.get('estimatedHours'),
        'estimatedCost': task.get('estimatedCost'),
        'customFieldValues': task.get('customFieldValues', []),
        'extensionFieldValues': task.get('extensionFieldValues', []),
        'timeEntryDateRange': task.get('timeEntryDateRange'),
        'timeAndMaterials': task.get('timeAndMaterials'),
        'keyValues': task.get('keyValues', []),
        'historicalKeyValues': task.get('historicalKeyValues', []),
        'costTypeUri': task.get('costTypeUri'),
    }


def _build_closed_task_def(task_data, child_tasks=None):
    """Build a closed task definition preserving all existing Replicon values.

    Used for orphan tasks that need to be closed but not deleted.
    """
    return {
        "task": {
            "target": {
                "uri": task_data['uri'],
                "name": None,
                "parent": None,
                "parameterCorrelationId": None
            },
            "name": task_data['name'],
            "code": task_data['code'],
            "description": None,
            "timeEntryDateRange": task_data.get('timeEntryDateRange'),
            "percentCompleted": task_data.get('percentCompleted', '0'),
            "isTimeEntryAllowed": "false",
            "estimatedHours": task_data.get('estimatedHours'),
            "isClosed": "true",
            "customFieldValues": task_data.get('customFieldValues', []),
            "extensionFieldValues": task_data.get('extensionFieldValues', []),
            "estimatedCost": task_data.get('estimatedCost'),
            "costTypeUri": task_data.get('costTypeUri'),
            "assignedResources": [],
            "timeAndMaterials": task_data.get('timeAndMaterials'),
            "keyValues": task_data.get('keyValues', []),
            "historicalKeyValues": task_data.get('historicalKeyValues', []),
        },
        "childTasks": child_tasks or []
    }

def get_time(costpoint_time_zone):
    costpoint_time_zone = timezone(costpoint_time_zone)
    datetime_in_timezone = datetime.fromisoformat(
        rail.result('get_last_run_date')).astimezone(costpoint_time_zone)
    return (datetime_in_timezone).replace(tzinfo=None).isoformat()

def get_filters(costpoint_time_zone):
    return [
        # TC_PROJ_FL Values:
        #     T=Time Collection
        #     E=Expense
        #     B=Time & Expense
        #     N=None
        {
            "name": "TC_PROJ_FL",
            "relation": "!=",
            "value": "E"
        },
        {
            "name": "TC_PROJ_FL",
            "relation": "!=",
            "value": "N"
        },
        {
            "name": "PJMBASIC_PROJ_LAST_MODIFIED",
            "relation": "gt=",
            "value": get_time(costpoint_time_zone)
        }
    ]

def get_project_filter_items(costpoint_time_zone):
    items = []
    last_item = []
    a_to_z_chars = list(map(chr, range(ord('A'), ord('Z')+1)))
    for item in a_to_z_chars:
        items.append([
            {
                "name": "PROJ_NAME",
                "relation": "like%",
                "value": item
            }
        ] + get_filters(costpoint_time_zone))
        last_item.append({
            "name": "PROJ_NAME",
            "relation": "not like%",
            "value": item
        })
    last_item = last_item + get_filters(costpoint_time_zone)
    items.append(last_item)
    return items

def get_project_data(dag_run):
    """Extract and return project data from DAG run configuration"""
    root_project_id = dag_run.conf['item']['root_project_id']
    data = list(
        map(lambda x: x['row']['data'], rail.result('get_costpoint_projects')))
    root_project_info = next(filter(
        lambda x: x['PROJ_ID'] == root_project_id, data), None)

    return root_project_id, data, root_project_info

def map_workforce_empid():
    """Extract unique employee IDs from workforce data"""
    data = list(set(map(
        lambda x: x['row']['data'].get('EMPL_ID'), 
        filter(
            lambda x: x['row']['data'].get('EMPL_ID'),
            list(itertools.chain(
                *list(map(lambda x: x['row']['children'], rail.result('get_workforce_user_costpoint')))
            ))
        )
    )))
    return data


def get_new_task_name(data, task_code):
    """Generate new task name based on tech spec requirements"""
    # Find task in CostPoint project data
    matching_tasks = list(filter(lambda x: x['PROJ_ID'] == task_code, data))
    
    # If task not found in project data, it might be a PLC task
    # PLC tasks don't need renaming as they follow PLC master data
    if not matching_tasks:
        return None  # Skip renaming for PLCs and non-existent tasks
    
    task_info = matching_tasks[0]
    
    # Apply task naming logic as per spec
    if task_info.get('ALLOW_CHARGES_FL') == 'N':
        return "/"
    else:
        # Use project name, append ID only for duplicates
        tasks_by_name = list(filter(lambda x: x['PROJ_NAME'] == task_info['PROJ_NAME'], data))
        return task_info['PROJ_NAME'] if len(tasks_by_name) == 1 else f"{task_info['PROJ_NAME']}_{task_info['PROJ_ID']}"


def get_assigned_resource_param_task(item, has_children=False):
    """Generate assigned resource parameters for tasks with complete structure"""
    # For parent tasks (non-bottom level), return empty list (no default assignment)
    if has_children or item['PROJ_WORK_FRC_FL'] != 'Y':
        return []

    # For bottom-level tasks, assign users directly with full structure
    assigned_resources = []
    workforce_children = next(map(
        lambda x: x['row']['children'],
        filter(
            lambda x: x['row']['data']['PROJ_ID'] == item['PROJ_ID'],
            rail.result('get_workforce_user_costpoint')
        )
    ), [])

    for child in workforce_children:
        emp_id = child['row']['data'].get('EMPL_ID')
        if emp_id:
            user_detail = rail.find_first_by_attr_and_get_attr(
                rail.result('get_users_from_replicon'),
                'employeeId',
                emp_id,
                'userDetails'
            )
            if user_detail:
                assigned_resources.append(_build_assigned_resource_dict(user_detail['uri']))

    return assigned_resources


def find_workforce_entry_recursive(row_data, proj_id):
    """Recursively search for a PJM_PROJEMPL_HDR entry with exact matching PROJ_ID.

    The workforce data has nested PJM_PROJEMPL_HDR entries for child tasks.
    This function finds the exact entry for the given proj_id.

    Returns:
        dict: The row_data of the matching entry, or None if not found
    """
    # Check if this row is a PJM_PROJEMPL_HDR with exact matching PROJ_ID
    if row_data.get('rsId') == 'PJM_PROJEMPL_HDR':
        if row_data.get('data', {}).get('PROJ_ID') == proj_id:
            return row_data

    # Recursively search children
    for child in row_data.get('children', []):
        result = find_workforce_entry_recursive(child.get('row', {}), proj_id)
        if result:
            return result

    return None


def find_plc_codes_in_entry(row_data, plc_codes):
    """Recursively search within a workforce entry to find all PLC codes.

    Stops at nested PJM_PROJEMPL_HDR to avoid crossing into child task's PLCs.
    """
    # Check if this row has a PLC code (PJM_PROJEMPLLABCAT_PLCWK row)
    if row_data.get('data', {}).get('BILL_LAB_CAT_CD'):
        plc_codes.add(row_data['data']['BILL_LAB_CAT_CD'])

    # Recursively search children, but STOP at nested PJM_PROJEMPL_HDR
    for child in row_data.get('children', []):
        child_row = child.get('row', {})
        # Don't recurse into nested PJM_PROJEMPL_HDR (that's a child task's data)
        if child_row.get('rsId') != 'PJM_PROJEMPL_HDR':
            find_plc_codes_in_entry(child_row, plc_codes)


def get_assigned_plcs_for_task(dag_run, proj_id):
    """Get PLCs explicitly assigned to a specific project/task.

    PLCs are only returned if directly assigned to the exact task (not inherited from parents).
    This handles the nested PJM_PROJEMPL_HDR structure in workforce data.

    Returns:
        tuple: (list of assigned PLCs, bool indicating if workforce entry exists)
    """
    assigned_plcs = set()
    workforce_data = rail.result('get_workforce_user_costpoint') or []

    # Search for the workforce entry with exact matching PROJ_ID (can be nested)
    workforce_entry = None
    for workforce_item in workforce_data:
        workforce_entry = find_workforce_entry_recursive(workforce_item.get('row', {}), proj_id)
        if workforce_entry:
            break

    if not workforce_entry:
        # No workforce data for this task
        return ([], False)

    # Found the entry - extract all PLC codes within it (but not from nested child tasks)
    plc_codes = set()
    for child in workforce_entry.get('children', []):
        child_row = child.get('row', {})
        if child_row.get('rsId') != 'PJM_PROJEMPL_HDR':
            find_plc_codes_in_entry(child_row, plc_codes)

    # Get PLC details for each found code
    for plc_code in plc_codes:
        plc_detail = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['plc_data'], 'code', plc_code
        )
        if plc_detail:
            assigned_plcs.add((plc_code, plc_detail['name']))

    return ([{'code': code, 'name': name} for code, name in assigned_plcs], True)


def find_plc_users_in_entry(row_data, plc_code, emp_ids):
    """Recursively search within a workforce entry to find employee IDs for a specific PLC.

    Stops at nested PJM_PROJEMPL_HDR to avoid crossing into child task's data.
    """
    # Check if this row has the matching PLC code
    if row_data.get('data', {}).get('BILL_LAB_CAT_CD') == plc_code:
        emp_id = row_data['data'].get('PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID')
        if emp_id:
            emp_ids.add(emp_id)

    # Recursively search children, but STOP at nested PJM_PROJEMPL_HDR
    for child in row_data.get('children', []):
        child_row = child.get('row', {})
        if child_row.get('rsId') != 'PJM_PROJEMPL_HDR':
            find_plc_users_in_entry(child_row, plc_code, emp_ids)


def get_plc_assigned_resources(proj_id, plc_code):
    """Get users assigned to specific PLC for a project with complete structure.

    This handles the nested PJM_PROJEMPL_HDR structure in workforce data.
    """
    assigned_users = []
    workforce_data = rail.result('get_workforce_user_costpoint') or []

    # Search for the workforce entry with exact matching PROJ_ID (can be nested)
    workforce_entry = None
    for workforce_item in workforce_data:
        workforce_entry = find_workforce_entry_recursive(workforce_item.get('row', {}), proj_id)
        if workforce_entry:
            break

    if not workforce_entry:
        return []

    # Found the entry - find all employee IDs for this PLC
    emp_ids = set()
    for child in workforce_entry.get('children', []):
        child_row = child.get('row', {})
        if child_row.get('rsId') != 'PJM_PROJEMPL_HDR':
            find_plc_users_in_entry(child_row, plc_code, emp_ids)

    # Get user details for each employee ID with complete structure
    for emp_id in emp_ids:
        user_detail = rail.find_first_by_attr_and_get_attr(
            rail.result('get_users_from_replicon'),
            'employeeId',
            emp_id,
            'userDetails'
        )
        if user_detail:
            assigned_users.append(_build_assigned_resource_dict(user_detail['uri']))

    return assigned_users


def should_create_project_leader_permission():
    """Check if project leader permission should be created"""
    return [1] if rail.result('get_project_leader_info_from_replicon') else []


def filter_tasks_needing_rename():
    """Filter tasks that need renaming"""
    return list(filter(
        lambda x: x['new_name'] is not None and x['name'] != x['new_name'], 
        rail.result('get_task_info_from_replicon')
    ))


def get_dag_run_data_items(dag_run):
    """Get DAG run configuration data items"""
    return dag_run.conf['item']['data']


def do_get_last_run_date(last_run_date_var_name, time_zone='UTC'):
    """Get the last run date for incremental sync"""
    # Get current time in specified timezone minus 2 seconds
    current_time = pendulum.now(time_zone).subtract(seconds=2)
    
    # Get the stored timestamp value
    lookup_timestamp_value = Variable.get(last_run_date_var_name, default_var=None)
    
    # Parse the stored value or use current time
    if lookup_timestamp_value:
        last_run_date_dt = pendulum.parse(lookup_timestamp_value)
    else:
        last_run_date_dt = current_time
    
    # Store current time for next run
    rail.set_result(current_time.isoformat(), 'current_time')
    
    # Return the last run date in ISO format
    return last_run_date_dt.isoformat()


def get_new_client_names():
    """Extract new client names that need to be created in Replicon"""
    # Get project data from either chunked or regular results
    project_data = rail.result('get_modified_projects') or rail.result('get_modified_projects_in_chunks') or []

    # Extract all client names from projects
    all_client_names = []
    for project in project_data:
        client_name = project['row']['data'].get('CUST_NAME')
        if client_name:  # Only add non-null client names
            all_client_names.append(client_name)

    # Get unique client names
    unique_client_names = set(all_client_names)

    # Get existing clients in Replicon
    existing_clients = set(rail.result('get_all_clients_from_replicon') or [])

    # Find new clients that need to be created
    new_clients = list(unique_client_names - existing_clients)

    return new_clients


# ============================================================================
# Orphan Detection and Task Lookup Functions (using URI for unique identification)
# ============================================================================

def get_existing_children_for_parent(parent_uri):
    """Get all existing children from Replicon for a specific parent URI.

    Args:
        parent_uri: The parent's URI (unique identifier in Replicon)

    Returns:
        List of existing child tasks with all preservable fields
    """
    replicon_tasks = rail.result('get_task_info_from_replicon') or []

    return [_extract_task_fields(task) for task in replicon_tasks if task.get('parent_uri') == parent_uri]


def get_existing_task_by_code(task_code):
    """Get existing task details from Replicon by task code.

    Args:
        task_code: The task's code in Replicon

    Returns:
        Task data dict with all preservable fields, or None if not found
    """
    replicon_tasks = rail.result('get_task_info_from_replicon') or []

    for task in replicon_tasks:
        if task['code'] == task_code:
            return _extract_task_fields(task)

    return None


def get_replicon_uri_by_code(task_code):
    """Find a Replicon task's or project's URI by its code.

    Checks both the root project and all tasks.
    """
    # Check if this is the root project
    project_details = rail.result('get_project_details')
    if project_details:
        project = project_details.get('project', {})
        if project.get('code') == task_code:
            return project.get('uri')

    # Search in tasks
    replicon_tasks = rail.result('get_task_info_from_replicon') or []
    for task in replicon_tasks:
        if task['code'] == task_code:
            return task['uri']
    return None


def get_orphan_children_recursive(dag_run, parent_uri):
    """Recursively get all children of an orphan task (for closing).

    When a parent is orphaned, all its children must also be closed.
    Preserves existing Replicon values like percentCompleted, estimatedHours, etc.

    Args:
        dag_run: The DAG run context
        parent_uri: The orphan parent's URI

    Returns:
        List of child task definitions with isClosed: true, preserving other values
    """
    replicon_tasks = rail.result('get_task_info_from_replicon') or []

    child_tasks = []
    for task in replicon_tasks:
        # Find direct children by parent_uri
        if task.get('parent_uri') == parent_uri:
            # Recursively get this child's children (using its URI)
            grandchildren = get_orphan_children_recursive(dag_run, task['uri'])
            child_tasks.append(_build_closed_task_def(task, grandchildren))

    return child_tasks