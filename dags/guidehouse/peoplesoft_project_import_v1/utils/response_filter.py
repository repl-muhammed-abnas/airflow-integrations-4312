import itertools
import rail

def extract_project_uri_from_rest_api(response):
    if not response:
        raise ValueError("Empty response from project creation")

    if isinstance(response, list) and len(response) > 0:
        response = response[0]

    if response.get('uri'):
        return response['uri']

    if response.get('error'):
        raise ValueError(f"REST API project creation failed: {response.get('error')}")

    raise ValueError("Project URI not found in response")

def format_existing_tasks(response):
    if not response:
        return []

    formatted_tasks = []
    for task in response:
        # Extract task status from isClosed field
        is_closed = task.get('isClosed', False)
        activity_status = 'I' if is_closed else 'A'

        # Extract dates from timeEntryDateRange
        start_date = ''
        end_date = ''
        date_range = task.get('timeEntryDateRange', {})

        if date_range.get('startDate'):
            start_obj = date_range['startDate']
            start_date = f"{start_obj.get('year', '')}-{start_obj.get('month', ''):02d}-{start_obj.get('day', ''):02d}"

        if date_range.get('endDate'):
            end_obj = date_range['endDate']
            end_date = f"{end_obj.get('year', '')}-{end_obj.get('month', ''):02d}-{end_obj.get('day', ''):02d}"

        # Extract Task Type from customFields
        task_type = ''
        custom_fields = task.get('customFields', [])
        for cf in custom_fields:
            custom_field = cf.get('customField', {})
            if custom_field.get('displayText') == 'Task Type':
                task_type = cf.get('text', '')
                break

        formatted_tasks.append({
            "name": task.get('name', ''),
            "code": task.get('code', ''),
            "uri": task.get('uri', ''),
            "activity_status": activity_status,
            "activity_start_date": start_date,
            "activity_end_date": end_date,
            "activity_type": task_type,
            "isClosed": is_closed
        })

    return formatted_tasks

def get_client_data(response, dag_run):
    client_name = dag_run.conf.get('client_name')
    client_id = dag_run.conf.get('client_id')

    if not client_name:
        return {
            'has_required_data': False,
            'exists': False,
            'client_uri': '',
            'validation_error': f'Client processing skipped: Client name is required but client_id "{client_id}" was provided without client_name'
        }

    if not response:
        return {
            'has_required_data': True,
            'exists': False,
            'client_uri': '',
            'validation_error': ''
        }

    found_client = rail.find_first_by_attr_and_get_attr(
        response,
        'displayText',
        client_name,
        'uri'
    )

    if not found_client:
        return {
            'has_required_data': True,
            'client_uri': '',
            'exists': False,
            'validation_error': ''
        }

    return {
        'has_required_data': True,
        'exists': True,
        'client_uri': found_client,
        'validation_error': ''
    }

def validate_project_manager_enabled(pm_user_details):
    if not pm_user_details:
        return {
            'is_enabled': False,
            'user_details': None,
            'validation_error': 'Project manager not found in Replicon'
        }

    is_enabled = pm_user_details.get('isEnabled', False)
    employee_id = pm_user_details.get('employeeId', 'Unknown')
    display_name = pm_user_details.get('displayText', 'Unknown')

    if not is_enabled:
        return {
            'is_enabled': False,
            'user_details': pm_user_details,
            'validation_error': f'Project manager {display_name} (Employee ID: {employee_id}) is disabled in Replicon and cannot be assigned to project'
        }

    return {
        'is_enabled': True,
        'user_details': pm_user_details,
        'validation_error': ''
    }

def get_co_manager_users_from_response(response):
    if not response:
        return {
            'enabled_user_uris': [],
            'disabled_users': [],
            'total_requested': 0,
            'enabled_count': 0
        }

    enabled_user_uris = []
    disabled_users = []
    total_requested = len(response)

    for i, user in enumerate(response):
        if not isinstance(user, dict):
            continue

        user_details = user.get('userDetails')
        if user_details and isinstance(user_details, dict):
            is_enabled = user_details.get('isEnabled', False)
            employee_id = user_details.get('employeeId', 'Unknown')
            display_name = user_details.get('displayText', 'Unknown')
            uri = user_details.get('uri')

            if is_enabled and uri:
                enabled_user_uris.append(uri)
            elif not is_enabled:
                disabled_users.append({
                    'employee_id': employee_id,
                    'display_name': display_name,
                    'reason': 'User is disabled in Replicon'
                })

    return {
        'enabled_user_uris': enabled_user_uris,
        'disabled_users': disabled_users,
        'total_requested': total_requested,
        'enabled_count': len(enabled_user_uris)
    }

def filter_all_divisions_data(response):
    """Format division data from paging response (includes all divisions for existence check)

    Note: DEPT_CODE from CSV corresponds to division description field in Replicon
    """
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    # Include ALL divisions (enabled and disabled) for existence check
    costcenter_info = list(map(lambda row: {
        'name': row['cells'][1]['textValue'],  # Name column (cells[1])
        'enabled': row['cells'][0]['textValue'],  # Effectively enabled column (cells[0])
        'description': row['cells'][2]['textValue'] if row['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else '',  # Description column (cells[2]) - corresponds to DEPT_CODE from CSV
    }, flaten_rows))
    return costcenter_info if costcenter_info else []
