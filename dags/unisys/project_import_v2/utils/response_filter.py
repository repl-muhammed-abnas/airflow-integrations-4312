"""
Unisys Project Import - Response Filter Utilities
Processes Replicon API responses for Unisys-specific requirements
"""


def extract_project_uri_from_rest_api(response):
    """
    Extract project URI from REST API CreateProjectOrApplyModifications response

    Args:
        response: REST API response from CreateProjectOrApplyModifications

    Returns:
        str: Project URI if successful, raises error if failed
    """
    if not response:
        raise ValueError("Empty response from project creation")

    # Handle if response is a list
    if isinstance(response, list) and len(response) > 0:
        response = response[0]

    # REST API returns the project object directly with uri
    if response.get('uri'):
        return response['uri']

    # Check for error in response
    if response.get('error'):
        raise ValueError(f"REST API project creation failed: {response.get('error')}")

    raise ValueError("Project URI not found in response")


def extract_project_uri_from_graphql(response):
    """
    Extract project URI from GraphQL addProject2 response

    Args:
        response: GraphQL response from addProject2 mutation

    Returns:
        str: Project URI if successful, None if error
    """
    if not response:
        return None

    # Handle if response is a list
    if isinstance(response, list) and len(response) > 0:
        response = response[0]

    # Check for data and successful project creation
    if response.get('data') and response['data'].get('addProject2'):
        project_data = response['data']['addProject2']

        # Check for errors first
        if project_data.get('errors') and len(project_data['errors']) > 0:
            error_msg = project_data['errors'][0].get('displayText', 'Unknown error')
            raise ValueError(f"GraphQL project creation failed: {error_msg}")

        # Get project URI
        if project_data.get('project') and project_data['project'].get('uri'):
            return project_data['project']['uri']

    # Check for GraphQL errors at top level
    if response.get('errors') and len(response['errors']) > 0:
        error_msg = response['errors'][0].get('message', 'Unknown GraphQL error')
        raise ValueError(f"GraphQL error: {error_msg}")

    return None


def get_project_data_from_list_service(response):
    """Extract project data from ProjectListService response"""
    if not response or not response.get('rows'):
        return None

    # Filter out null cells (urn:replicon:list-type:null)
    filtered_rows = [
        row for row in response['rows']
        if not any(
            cell.get('dataType') == 'urn:replicon:list-type:null'
            for cell in row.get('cells', [])
        )
    ]

    if not filtered_rows:
        return None

    projects = list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "code": row['cells'][1]['textValue'] if len(row['cells']) > 1 else '',
        "uri": row['cells'][0]['uri']
    }, filtered_rows))

    return projects[0] if projects else None


def format_existing_tasks(response):
    """
    Format existing task details from GetChildrenTaskDetails response
    Handles Unisys field reversal - remember:
    - Replicon task 'name' field contains CSV task code
    - Replicon task 'code' field contains CSV task name
    """
    if not response:
        return []

    formatted_tasks = []
    for task in response:
        # Extract paycode from custom fields
        paycode = '105'  # Default
        if task.get('customFields'):
            for cf in task['customFields']:
                if 'paycode' in cf.get('displayText', '').lower():
                    paycode = cf.get('value', '105')
                    break

        formatted_tasks.append({
            "name": task.get('name', ''),  # This contains CSV task code due to field reversal
            "code": task.get('code', ''),  # This contains CSV task name due to field reversal
            "uri": task.get('uri', ''),
            "startDate": task.get('startDate', ''),
            "endDate": task.get('endDate', ''),
            "paycode": paycode
        })

    return formatted_tasks

