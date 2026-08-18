from datetime import datetime
from os import path
import rail
import re
from guidehouse.peoplesoft_project_import.utils import request_payload

def validate_project_file_name(file_path, expected_prefix):
    file_name = path.basename(file_path)
    project_pattern = rf'^{expected_prefix}_Project_\d{{14}}\.txt\.pgp$'
    resource_pattern = rf'^{expected_prefix}_Project_team_\d{{12}}\.txt\.pgp$'
 
    if re.match(project_pattern, file_name):
        return {
            'is_valid': True,
            'is_resource_file': False,
            'is_unknown_file': False,
            'error_message': ''
        }
    elif re.match(resource_pattern, file_name):
        return {
            'is_valid': False,
            'is_resource_file': True,
            'is_unknown_file': False,
            'error_message': f"File is a resource assignment file: {file_name}. This will be processed by resource assignment DAG."
        }
    else:
        return {
            'is_valid': False,
            'is_resource_file': False,
            'is_unknown_file': True,
            'error_message': f"Invalid file name: {file_name}. Expected PeopleSoft project format: {expected_prefix}_Project_YYYYMMDDHHMMSS.txt.pgp or resource format: {expected_prefix}_Project_team_YYYYMMDDHHSS.txt.pgp"
        }

def _is_invalid_replicon_date(date_string, DATE_FORMAT_INPUT):
    """
    Detect invalid or sentinel date values that Replicon cannot process:
    - Year 0001 or earlier (too old)
    - Year 9999 or later (future sentinel)
    - Years before 1900 or after 2200 (reasonable business bounds)
    """
    if not date_string:
        return False

    normalized = date_string.strip()

    try:
        parsed = datetime.strptime(normalized, DATE_FORMAT_INPUT)
        # Reject dates outside reasonable business range (catches sentinel dates like 0001-01-01 and 9999-12-31)
        return parsed.year < 1900 or parsed.year > 2200
    except (ValueError, TypeError):
        return False

def _get_length_error(field_name, value, max_length=255):
    if value is None:
        return None
    val = str(value)
    if len(val) > max_length:
        return f"{field_name} exceeds maximum length of {max_length} characters ({len(val)} received)"
    return None

def validate_project_dates_only(project_data, DATE_FORMAT_INPUT, max_field_length):
    errors = []
    project_start = None
    project_end = None
    project_id = project_data.get('project_id', 'Unknown')

    project_descr = project_data.get('project_descr', '').strip()
    activity_descr = project_data.get('activity_descr', '').strip()

    length_error = _get_length_error('Project Description', project_descr, max_field_length)
    if length_error:
        errors.append(f"Project {project_id} is skipped due to invalid project description: {length_error}")

    length_error = _get_length_error('Task Description', activity_descr, max_field_length)
    if length_error:
        errors.append(f"Project {project_id} is skipped due to invalid activity description: {length_error}")

    if project_data.get('project_start_date'):
        if _is_invalid_replicon_date(project_data['project_start_date'], DATE_FORMAT_INPUT):
            errors.append(f"Project {project_id} is skipped due to invalid project start date value '{project_data['project_start_date']}'")
        else:
            try:
                project_start = datetime.strptime(project_data['project_start_date'], DATE_FORMAT_INPUT)
            except ValueError:
                date_str = project_data['project_start_date']
                errors.append(f"Project {project_id} is skipped due to invalid start date format received '{date_str}', expected format YYYY-MM-DD (e.g., 2026-02-01)")

    if project_data.get('project_end_date'):
        if _is_invalid_replicon_date(project_data['project_end_date'], DATE_FORMAT_INPUT):
            errors.append(f"Project {project_id} is skipped due to invalid project end date value '{project_data['project_end_date']}'")
        else:
            try:
                project_end = datetime.strptime(project_data['project_end_date'], DATE_FORMAT_INPUT)
            except ValueError:
                date_str = project_data['project_end_date']
                errors.append(f"Project {project_id} is skipped due to invalid end date format received '{date_str}', expected format YYYY-MM-DD (e.g., 2026-12-31)")

    if project_start and project_end:
        if project_end < project_start:
            errors.append(f"Project {project_id} is skipped due to invalid date sequence, end date '{project_data['project_end_date']}' received before start date '{project_data['project_start_date']}'")

    return {
        'is_valid': len(errors) == 0,
        'errors': errors,
        'project_start': project_data.get('project_start_date'),
        'project_end': project_data.get('project_end_date')
    }

def validate_task_dates(task_data, DATE_FORMAT_INPUT, project_start=None, project_end=None):

    result = {
        'is_valid': True,
        'task': task_data,
        'error': None
    }

    activity_code = task_data.get('activity', 'Unknown')
    project_id = task_data.get('project_id', 'Unknown')
    task_start = None
    task_end = None
    task_name = task_data.get('activity_descr', '').strip()

    length_error = _get_length_error('Task Description', task_name)
    if length_error:
        result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to invalid activity description: {length_error}"
        result['is_valid'] = False
        return result

    if task_data.get('activity_start_date'):
        if _is_invalid_replicon_date(task_data['activity_start_date'], DATE_FORMAT_INPUT):
            result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to invalid activity start date value '{task_data['activity_start_date']}'"
            result['is_valid'] = False
            return result
        try:
            task_start = datetime.strptime(task_data['activity_start_date'], DATE_FORMAT_INPUT)
        except ValueError:
            date_str = task_data['activity_start_date']
            result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to invalid start date format received '{date_str}', expected format YYYY-MM-DD (e.g., 2026-02-01)"
            result['is_valid'] = False
            return result

    if task_data.get('activity_end_date'):
        if _is_invalid_replicon_date(task_data['activity_end_date'], DATE_FORMAT_INPUT):
            result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to invalid activity end date value '{task_data['activity_end_date']}'"
            result['is_valid'] = False
            return result
        try:
            task_end = datetime.strptime(task_data['activity_end_date'], DATE_FORMAT_INPUT)
        except ValueError:
            date_str = task_data['activity_end_date']
            result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to invalid end date format received '{date_str}', expected format YYYY-MM-DD (e.g., 2026-12-31)"
            result['is_valid'] = False
            return result

    if task_start and task_end:
        if task_end < task_start:
            result['is_valid'] = False
            result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to invalid date sequence, end date '{task_data['activity_end_date']}' received before start date '{task_data['activity_start_date']}'"
            return result

    project_start_dt = None
    project_end_dt = None

    if project_start:
        try:
            project_start_dt = datetime.strptime(project_start, DATE_FORMAT_INPUT)
        except (ValueError, AttributeError):
            project_start_dt = None

    if project_end:
        try:
            project_end_dt = datetime.strptime(project_end, DATE_FORMAT_INPUT)
        except (ValueError, AttributeError):
            project_end_dt = None

    if project_start_dt and task_start:
        if task_start < project_start_dt:
            result['is_valid'] = False
            result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to task start date '{task_data['activity_start_date']}' falling before project start date '{project_start}'"
            return result

    if project_end_dt and task_end:
        if task_end > project_end_dt:
            result['is_valid'] = False
            result['error'] = f"Task {activity_code} in Project {project_id} is skipped due to task end date '{task_data['activity_end_date']}' falling after project end date '{project_end}'"
            return result

    return result

def parse_co_managers(co_manager_string):
    if not co_manager_string:
        return []

    # Split by semicolon and clean each ID
    co_managers = []
    for manager_id in co_manager_string.split(';'):
        cleaned_id = manager_id.strip()
        if cleaned_id:
            co_managers.append(cleaned_id)

    return co_managers

def should_update_co_manager_assignments():
    """
    Compare all co-managers (CSV + parent) with existing project sharing assignments.
    Returns True if there are new co-managers to add, False if all are already assigned.
    """
    # Get all co-managers (CSV + parent project managers)
    all_co_managers = get_all_co_manager_uris_for_permission_check()
    all_co_manager_uris = all_co_managers.get('userUris', [])

    if not all_co_manager_uris:
        return False  # No co-managers to assign

    # Get existing sharing assignments from project
    existing_assignments = rail.result("get_existing_sharing_assignments", [])
    existing_user_uris = set()

    for assignment in existing_assignments:
        if assignment.get('user', {}).get('uri'):
            existing_user_uris.add(assignment['user']['uri'])

    # Check if all co-managers (CSV + parent) are already assigned
    all_co_manager_uris_set = set(all_co_manager_uris)

    # If all co-managers are already in existing assignments, no need to update
    if all_co_manager_uris_set.issubset(existing_user_uris):
        return False

    # Some co-managers (CSV or parent) are missing, need to update
    return True

def get_invalid_logs_property_conf(item):
    def get_missing_field():
        missing_fields = []
        field_display_names = {
            'project_id': 'Project ID',
            'project_descr': 'Project Description',
            'project_status': 'Project Status',
            'activity_status': 'Activity Status',
            'project_start_date': 'Project Start Date',
            'activity_start_date': 'Activity Start Date',
            'enforce': 'Enforce'
        }

        mandatory_fields = {
            "project_fields": {
                "PROJECT_ID": "project_id",
                "PROJECT_DESCR": "project_descr",
                "PROJECT_START_DATE": "project_start_date",
                "ACTIVITY_START_DATE": "activity_start_date"
                # Note: project_status, activity_status, and enforce have special validation below
            }
        }

        for csv_field, db_field in mandatory_fields["project_fields"].items():
            if item.get(db_field) in [None, '']:
                missing_fields.append(field_display_names.get(db_field, csv_field))

        project_status = item.get('project_status', '').strip().upper()
        if not project_status:
            missing_fields.append('Project Status')
        elif project_status not in ['A', 'I']:
            status = item.get('project_status', 'blank')
            return f"Project {item.get('project_id', 'Unknown')} is skipped due to invalid project status '{status}' received, only 'A' (Active) or 'I' (Inactive) allowed"

        activity_status = item.get('activity_status', '').strip().upper()
        if not activity_status:
            missing_fields.append('Activity Status')
        elif activity_status not in ['A', 'I']:
            status = item.get('activity_status', 'blank')
            return f"Project {item.get('project_id', 'Unknown')} is skipped due to invalid activity status '{status}' received, only 'A' (Active) or 'I' (Inactive) allowed"

        enforce = item.get('enforce', '').strip().upper()
        if not enforce:
            missing_fields.append('Enforce')
        elif enforce not in ['YES', 'NO']:
            return f"Project {item.get('project_id', 'Unknown')} is skipped due to invalid enforce value '{item.get('enforce', 'blank')}' received, only 'YES' or 'NO' allowed"

        if missing_fields:
            fields_str = ' and '.join(missing_fields) if len(missing_fields) <= 2 else ', '.join(missing_fields[:-1]) + ' and ' + missing_fields[-1]
            return f"Project processing is skipped due to required fields missing in CSV file: {fields_str}"

        return "Project processing is skipped due to validation errors"

    return {
        "client_id": item.get('bill_to_cust_id', ''),
        "client_name": item.get('customer_name', ''),
        "project_id": item.get('project_id', ''),
        "project_name": item.get('project_descr', ''),
        "task_code": item.get('activity', ''),
        "task_name": item.get('activity_descr', ''),
        "action": 'Validation',
        "status": 'Exception',
        "details": get_missing_field(),
        "enforce_value": item.get('enforce', '')
    }

def get_guidehouse_task_log_properties(base_data, task_data, action, status='Success', details=''):
    return {
        "client_id": base_data.get('bill_to_cust_id', ''),
        "client_name": base_data.get('customer_name', ''),
        "project_id": base_data.get('project_id', ''),
        "project_name": base_data.get('project_descr', ''),
        "task_code": task_data.get('activity', ''),
        "task_name": task_data.get('activity_descr', ''),
        "action": action,
        "status": status,
        "details": details,
        "enforce_value": base_data.get('enforce', '')
    }

def get_client_log_properties(dag_run, action, status, details):
    return {
        "client_id": dag_run.conf['client_id'],
        "client_name": dag_run.conf['client_name'],
        "project_id": '',  # Not applicable for client operations
        "project_name": '',  # Not applicable for client operations
        "task_code": '',   # Not applicable for client operations
        "task_name": '',   # Not applicable for client operations
        "action": action,
        "status": status,
        "details": details,
        "enforce_value": ''
    }

def format_logs_callable():
    """
    Format and aggregate logs from all DAG runs
    Calculates success, error, and exception counts for email reporting
    """
    final_log_records = []
    final_log_records.extend(rail.load_all_records(rail.result("create_exception_log")))

    # Set counters for email template
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'exception', final_log_records))))

    return rail.write_json_artifact(final_log_records)

def get_task_to_add_update_skip(DATE_FORMAT_INPUT):
    current_tasks_in_project = rail.result('get_all_tasks_for_project') if bool(rail.result('get_all_tasks_for_project')) else []
    tasks_to_process = rail.load_all_records(rail.result("get_project_data_from_query"))

    if not tasks_to_process:
        return {
            'add': [],
            'update': [],
            'skip': []
        }

    project_start = rail.result("validate_dates", {}).get('project_start')
    project_end = rail.result("validate_dates", {}).get('project_end')

    if not current_tasks_in_project:
        validation_result = validate_tasks_for_addition(tasks_to_process, DATE_FORMAT_INPUT, project_start, project_end)
        return {
            'add': validation_result['valid'],
            'update': [],
            'skip': validation_result['skipped']
        }

    tasks_to_add = []
    tasks_to_update = []
    tasks_to_skip = []

    for task in tasks_to_process:
        task_code = task.get('activity', 'Unknown')
        project_number = task.get('project_id', 'Unknown')
        task_name = task.get('activity_descr', '')

        task_validation = validate_task_dates(task, DATE_FORMAT_INPUT, project_start, project_end)
        if not task_validation['is_valid']:
            tasks_to_skip.append({
                "task": task,
                "message": task_validation['error'],
                "activity": task_code,
                "activity_descr": task_name,
                "action": "Skip",
                "status": "Skipped"
            })
            continue

        # ACTIVITY field validation - ACTIVITY required when ACTIVITY_DESC provided
        task_code_cleaned = task.get('activity', '').strip()
        task_name_cleaned = task.get('activity_descr', '').strip()

        if task_name_cleaned and not task_code_cleaned:
            tasks_to_skip.append({
                "task": task,
                "message": f"ACTIVITY (Task Name) is missing while ACTIVITY_DESCR {task_name_cleaned} is provided",
                "activity": task_code_cleaned or 'Unknown',
                "activity_descr": task_name_cleaned,
                "action": "Skip",
                "status": "Skipped"
            })
            continue

        existing_task = rail.find_first_by_attr_and_get_attr(
            current_tasks_in_project, "name", task['activity'])

        if existing_task:
            if can_update_task_guidehouse(existing_task, task):
                task['existing_uri'] = existing_task['uri']
                tasks_to_update.append(task)
            else:
                tasks_to_skip.append({
                    "task": task,
                    "message": f"Task {task_code} in Project {project_number} is skipped due to no data changes detected, existing Replicon task already matches Input values",
                    "activity": task_code,
                    "activity_descr": task_name,
                    "action": "Skip",
                    "status": "Skipped"
                })
        else:
            tasks_to_add.append(task)

    return {
        'add': rail.load_all_records(rail.write_json_artifact(tasks_to_add)) if tasks_to_add else tasks_to_add,
        'update': rail.load_all_records(rail.write_json_artifact(tasks_to_update)) if tasks_to_update else tasks_to_update,
        'skip': rail.load_all_records(rail.write_json_artifact(tasks_to_skip)) if tasks_to_skip else tasks_to_skip
    }

def convert_replicon_date_to_string(date_obj):
    if not date_obj or not isinstance(date_obj, dict):
        return ''

    try:
        year = date_obj.get('year', 0)
        month = date_obj.get('month', 0)
        day = date_obj.get('day', 0)

        if year and month and day:
            return f"{year}-{month:02d}-{day:02d}"
    except (ValueError, TypeError):
        pass

    return ''

def format_date_for_comparison(date_string):
    if not date_string:
        return ''
    if isinstance(date_string, str) and len(date_string) == 10:
        return date_string.strip()

    return ''

def can_update_task_guidehouse(existing_task, csv_task):
    """
    Determine if task needs update based on Guidehouse business rules
    Uses actual API response fields from format_existing_tasks()

    Compares:
    1. Task name (activity_descr vs code)
    2. Task status (activity_status A/I vs activity_status from isClosed)
    3. Start and end dates (YYYY-MM-DD format)
    4. Task type (activity_type vs custom field text)
    """
    if existing_task.get('code') != csv_task.get('activity_descr'):
        return True

    csv_status = csv_task.get('activity_status', '').upper()
    existing_status = existing_task.get('activity_status', '').upper()

    if csv_status != existing_status:
        return True

    existing_start = format_date_for_comparison(existing_task.get('activity_start_date'))
    existing_end = format_date_for_comparison(existing_task.get('activity_end_date'))
    csv_start = format_date_for_comparison(csv_task.get('activity_start_date'))
    csv_end = format_date_for_comparison(csv_task.get('activity_end_date'))

    if existing_start != csv_start or existing_end != csv_end:
        return True

    csv_task_type = csv_task.get('activity_type', '').upper()
    existing_task_type = existing_task.get('activity_type', '').upper()

    if csv_task_type != existing_task_type:
        return True

    return False

def validate_tasks_for_addition(tasks, DATE_FORMAT_INPUT, project_start=None, project_end=None):
    valid_tasks = []
    skipped_tasks = []

    for task in tasks:
        task_code = task.get('activity', '').strip()
        task_name = task.get('activity_descr', '').strip()

        if task_name and not task_code:
            skipped_tasks.append({
                "task": task,
                "message": f"ACTIVITY (Task Name) is missing while ACTIVITY_DESCR {task_name} is provided",
                "activity": task_code or 'Unknown',
                "activity_descr": task_name,
                "action": "Skip",
                "status": "Skipped"
            })
            continue

        task_validation = validate_task_dates(task, DATE_FORMAT_INPUT, project_start, project_end)
        if task_validation['is_valid']:
            valid_tasks.append(task)
        else:
            skipped_tasks.append({
                "task": task,
                "message": task_validation['error'],
                "activity": task_code or 'Unknown',
                "activity_descr": task_name,
                "action": "Skip",
                "status": "Skipped"
            })

    return {
        'valid': valid_tasks,
        'skipped': skipped_tasks
    }

def map_task_success_error(task_id, _type):
    action = "Added" if _type == "add" else "Updated"
    batched_results = rail.result(task_id, [])
    task_list = rail.result("get_all_task_to_add_update", {}).get(_type, [])

    flattened_results = []
    if isinstance(batched_results, list) and batched_results:
        if isinstance(batched_results[0], list):
            for batch_result in batched_results:
                flattened_results.extend(batch_result)
        else:
            flattened_results = batched_results
    else:
        flattened_results = []

    res = []
    for idx, task_res in enumerate(flattened_results):
        if idx >= len(task_list):
            break

        task_detail = task_list[idx].copy()
        status = "Success"
        msg = f"Task {action} Successfully"

        if task_res.get("error"):
            msg = ";".join([error.get('displayText', '')
                           for error in task_res.get("error", {}).get('notifications', [])])
            status = "Error"

        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_detail)

    return res

def split_list_into_batches(items, batch_size=500):
    batches = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batches.append(batch)
    return batches

def get_batched_tasks(action_type, batch_size=500):
    task_data = rail.result("get_all_task_to_add_update")
    tasks = task_data.get(action_type, [])

    if not tasks:
        return []

    return split_list_into_batches(tasks, batch_size)

def get_missing_permission_sets(assigned_permissions, project_mgmt_perm_set_uri):
    has_project_management = False

    if assigned_permissions:
        for assigned_perm in assigned_permissions:
            policy_uri = assigned_perm.get('policyUri', '')
            if policy_uri == 'urn:replicon:policy:project-management':
                has_project_management = True

    missing_permission_sets = []
    if not has_project_management and project_mgmt_perm_set_uri:
        missing_permission_sets.append(project_mgmt_perm_set_uri)

    return missing_permission_sets

def has_any_co_managers_combined():
    """
    Check if there are any co-managers (CSV + parent project combined).
    Used to determine if co-manager processing should occur.

    Returns:
        bool: True if any co-managers exist (CSV or parent project)
    """
    # Get combined total from parent + CSV co-managers
    combined_result = rail.result("combine_parent_and_csv_co_managers", {})
    combined_total = combined_result.get('combined_total', 0)

    return combined_total > 0

def are_any_co_managers_available():
    """
    Check if any co-managers are available/enabled (CSV + parent project).
    Used to determine if permission checking should occur.

    Returns:
        bool: True if any co-managers are available for permission checking
    """
    # Get all co-manager URIs (CSV + parent)
    uri_data = get_all_co_manager_uris_for_permission_check()
    all_uris = uri_data.get('userUris', [])

    return len(all_uris) > 0

def get_all_co_manager_uris_for_permission_check():
    """
    Get all co-manager URIs for permission checking (CSV + parent project).
    Used by BulkGetAssignedPermissionSetsForUsers API.

    Returns:
        Dict with userUris list for the bulk API call
    """
    all_co_manager_uris = []

    # Get CSV co-managers URIs (if any)
    co_manager_response = rail.result("get_co_managers_in_replicon", {})
    csv_co_manager_uris = co_manager_response.get('enabled_user_uris', [])
    all_co_manager_uris.extend(csv_co_manager_uris)

    # Get parent project co-manager URIs (if CP_PROJECT exists)
    combined_result = rail.result("combine_parent_and_csv_co_managers", {})
    parent_manager_uris = combined_result.get('parent_manager_uris', [])
    all_co_manager_uris.extend(parent_manager_uris)

    # Remove duplicates while preserving order
    unique_uris = []
    seen = set()
    for uri in all_co_manager_uris:
        if uri not in seen:
            unique_uris.append(uri)
            seen.add(uri)

    return {
        "userUris": unique_uris
    }

def get_co_manager_missing_permission_sets(co_manager_permissions_result, project_mgmt_perm_set_uri):
    """
    Check which co-managers are missing project management permissions.
    Updated to handle BulkGetAssignedPermissionSetsForUsers response.

    Args:
        co_manager_permissions_result: Response from BulkGetAssignedPermissionSetsForUsers
        project_mgmt_perm_set_uri: URI of project management permission set

    Returns:
        List of dicts with 'userUri' and 'permissionSetUri' for users needing permissions
    """
    users_needing_permissions = []

    if not project_mgmt_perm_set_uri or not co_manager_permissions_result:
        return users_needing_permissions

    # Group permissions by user URI
    user_permissions = {}
    for perm_entry in co_manager_permissions_result:
        user_uri = perm_entry.get('user', {}).get('uri')
        policy_uri = perm_entry.get('policyUri')

        if user_uri:
            if user_uri not in user_permissions:
                user_permissions[user_uri] = []
            user_permissions[user_uri].append(policy_uri)

    # Check each user for project management permissions
    for user_uri, policies in user_permissions.items():
        has_project_management = 'urn:replicon:policy:project-management' in policies

        if not has_project_management:
            users_needing_permissions.append({
                'userUri': user_uri,
                'permissionSetUri': project_mgmt_perm_set_uri
            })

    return users_needing_permissions

def create_resource_assignment_batches_from_response(api_response, tasks, batch_size=500):
    if not api_response:
        return []

    resource_uris = [item['resource']['uri'] for item in api_response]

    if not resource_uris or not tasks:
        return []

    resource_batches = split_list_into_batches(resource_uris, batch_size)

    assignment_batches = []
    for task in tasks:
        for resource_batch in resource_batches:
            assignment_batches.append({
                "task_uri": task['uri'],
                "resource_uris": resource_batch
            })

    return assignment_batches

def extract_newly_added_task_uris_from_batches():
    batched_results = rail.result("add_task_batches", [])
    task_uris = []

    if isinstance(batched_results, list):
        for batch in batched_results:
            if isinstance(batch, list):
                for task_result in batch:
                    if isinstance(task_result, dict) and task_result:
                        task_uri = task_result.get('task', {}).get('uri')
                        if task_uri:
                            task_uris.append({'uri': task_uri})

    return task_uris

def get_all_project_task_uris_for_enforce():
    all_task_uris = []

    try:
        newly_added_tasks = extract_newly_added_task_uris_from_batches()
        all_task_uris.extend(newly_added_tasks)
    except:
        pass

    try:
        existing_tasks = rail.result("get_all_tasks_for_project", [])
        for task in existing_tasks:
            if task.get('uri'):
                all_task_uris.append({'uri': task['uri']})
    except:
        pass

    unique_tasks = []
    seen_uris = set()
    for task in all_task_uris:
        if task['uri'] not in seen_uris:
            unique_tasks.append(task)
            seen_uris.add(task['uri'])

    return unique_tasks

# ========== IWO (Inter company worker order) Project Linking Functions ==========

def get_projects_for_lookup(dag_run):
    """Build project array for BulkGetProjectDetails3 - includes parent if CP_PROJECT present"""
    # Get project data from CSV (not dag_run.conf)
    project_data = rail.result("load_project_data_from_query")

    projects = [{"code": dag_run.conf.get('project_id')}]

    # Add parent project lookup if CP_PROJECT is present in CSV data
    cp_project = project_data.get('cp_project', '').strip()
    if cp_project:
        projects.append({"code": cp_project})

    return projects

def process_project_lookup_response(response):
    """Process combined project lookup response with parent project validation"""
    result = {'current_project': None, 'parent_project': None}

    if not response:
        return result

    # First result is always current project
    if len(response) > 0:
        result['current_project'] = response[0].get('projectDetails')

    # Second result is parent project if CP_PROJECT was included
    if len(response) > 1:
        result['parent_project'] = response[1].get('projectDetails')

        # Get CP_PROJECT value from CSV data for validation
        project_data = rail.result("load_project_data_from_query")
        cp_project = project_data.get('cp_project', '').strip()

        # If CP_PROJECT was provided but parent project doesn't exist, store error info for validation
        if cp_project and not result['parent_project']:
            result['parent_project_error'] = f"Parent project '{cp_project}' specified in CP_PROJECT field does not exist in Replicon"

    return result

def validate_parent_project_for_linking():
    """Validate parent project exists and can be linked"""

    project_details = rail.result("get_project_details")
    parent_project = project_details.get('parent_project')
    parent_project_error = project_details.get('parent_project_error')
    current_project_uri = request_payload.get_project_uri()
    cp_project_code = rail.result("load_project_data_from_query").get('cp_project', '')

    # Check if there was an error from project lookup
    if parent_project_error:
        return {
            'is_valid': False,
            'should_continue': False,
            'error': parent_project_error
        }

    if not parent_project:
        return {
            'is_valid': False,
            'should_continue': False,
            'error': f'Parent project with code "{cp_project_code}" not found in Replicon'
        }

    if parent_project.get('uri') == current_project_uri:
        return {
            'is_valid': False,
            'should_continue': False,
            'error': f'Cannot link project to itself (CP_PROJECT "{cp_project_code}" matches current project)'
        }

    return {
        'is_valid': True,
        'should_continue': True,
        'parent_uri': parent_project.get('uri'),
        'parent_name': parent_project.get('name', '')
    }

def extract_parent_project_leader():
    """Extract project leader URI from parent project details (only if parent project exists)"""
    project_details = rail.result("get_project_details")
    parent_project = project_details.get('parent_project', {})

    # Only extract project leader if parent project exists
    project_leader_uri = None
    if parent_project and parent_project.get('projectLeader'):
        project_leader_uri = parent_project['projectLeader'].get('uri')

    return {
        'parent_project_leader_uri': project_leader_uri,
        'has_parent_leader': bool(project_leader_uri),
        'has_parent_project': bool(parent_project)
    }

def extract_parent_project_manager_uris(sharing_assignments):
    """Extract manager URIs from parent project sharing assignments (co-managers only)"""
    if not sharing_assignments:
        return []

    manager_uris = []
    for assignment in sharing_assignments:
        user_uri = assignment.get('user', {}).get('uri')
        if user_uri:
            manager_uris.append(user_uri)

    return manager_uris

def combine_parent_and_csv_co_managers():
    """Combine parent project leader + co-managers with CSV co-managers (only if CP_PROJECT exists)"""

    # Get CSV co-managers employee IDs
    csv_co_managers = rail.result("load_project_data_from_query").get('co_manager', '').split(';')
    csv_co_managers = [mgr.strip() for mgr in csv_co_managers if mgr.strip()]

    # Check if we have a parent project (CP_PROJECT field)
    project_details = rail.result("get_project_details")
    has_parent_project = bool(project_details.get('parent_project'))

    parent_manager_uris = []
    parent_leader_uri = None
    parent_co_manager_uris = []

    # Only process parent project managers if CP_PROJECT exists
    if has_parent_project:
        # Get parent project leader URI
        parent_leader_data = rail.result("get_parent_project_leader", {})
        parent_leader_uri = parent_leader_data.get('parent_project_leader_uri')

        # Get parent project co-manager URIs
        parent_co_manager_uris = rail.result("get_parent_project_managers", [])

        # Combine parent leader + parent co-managers
        if parent_leader_uri:
            parent_manager_uris.append(parent_leader_uri)
        parent_manager_uris.extend(parent_co_manager_uris)

    return {
        'csv_co_manager_ids': csv_co_managers,
        'parent_manager_uris': parent_manager_uris,  # Empty if no parent project
        'parent_leader_uri': parent_leader_uri,
        'parent_co_manager_uris': parent_co_manager_uris,
        'has_parent_project': has_parent_project,
        'combined_total': len(csv_co_managers) + len(parent_manager_uris)
    }

def should_create_iwo_project_link():
    """Determine if IWO project link should be created"""

    validation = rail.result("validate_parent_project")
    if not validation.get('should_continue'):
        return False

    existing_links = rail.result("check_existing_project_links", [])
    parent_uri = validation.get('parent_uri')
    child_uri = request_payload.get_project_uri()  # Current child project URI

    # Check if link between parent and THIS child already exists
    for link in existing_links:
        base_uri = link.get('baseProject', {}).get('uri')
        target_uri = link.get('targetProject', {}).get('uri')

        # Check if there's already a link between parent and this specific child
        if ((base_uri == parent_uri and target_uri == child_uri) or
            (base_uri == child_uri and target_uri == parent_uri)):
            return False  # Link already exists between parent and child

    return True  # No link exists, create new link

def validate_optional_fields(project_data):
    """Validate optional field formats and return list of issues"""
    issues = []

    dept_code = project_data.get('dept_code', '').strip()
    dept_name = project_data.get('dept_name', '').strip()
    cp_project = project_data.get('cp_project', '').strip()

    # DEPT_NAME presence when DEPT_CODE exists
    if dept_code and not dept_name:
        issues.append(f"cost center assignment skipped (DEPT_NAME required when DEPT_CODE: '{dept_code}' is provided)")

    return issues
