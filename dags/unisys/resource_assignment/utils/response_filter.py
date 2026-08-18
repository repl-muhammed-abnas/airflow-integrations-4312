"""
Unisys Resource Assignment - Response Filter Utilities
Processes Replicon API responses for assignment operations
"""


def format_project_task_details(response):
    """
    Format task details from GetChildrenTaskDetails response
    Returns list of tasks with essential fields for assignment

    Args:
        response: Response from GetChildrenTaskDetails API

    Returns:
        list: List of task dictionaries with uri, name, code
    """
    return list(map(lambda task: {
        "task_name": task['name'],
        "task_code": task['code'],
        "uri": task['uri']
    }, response))


def extract_resource_allocations_from_graphql(response):
    """
    Extract resource allocations from GraphQL query response

    Returns dict mapping userUri to allocation details including allocation ID

    Args:
        response: GraphQL response from resourceAllocations query
                  Can be dict or list containing dict

    Returns:
        dict: Map of userUri to allocation details
              {
                  "urn:...:user:70": {
                      'allocation_id': 'urn:...:psa-resource-allocation:xxx',
                      'startDate': '2025-03-01T00:00:00.000Z',
                      'endDate': '2025-04-12T00:00:00.000Z',
                      'scheduleRules': [...],
                      'allocationStatus': 'COMMITTED'
                  }
              }
    """
    if not response:
        return {}

    # Handle if response is a dict directly or wrapped in a list
    if isinstance(response, dict):
        response_data = response
    elif isinstance(response, list) and len(response) > 0:
        response_data = response[0]
    else:
        return {}

    # Navigate through the nested structure
    if not response_data.get('data'):
        return {}

    if not response_data['data'].get('resourceAllocations'):
        return {}

    resource_allocations = response_data['data']['resourceAllocations'].get('resourceAllocations', [])

    if not resource_allocations:
        return {}

    allocation_map = {}

    for allocation in resource_allocations:
        if not allocation or not allocation.get('user'):
            continue

        user_uri = allocation['user'].get('userUri')
        if not user_uri:
            continue

        allocation_map[user_uri] = {
            'allocation_id': allocation.get('id'),
            'startDate': allocation.get('startDate'),
            'endDate': allocation.get('endDate'),
            'scheduleRules': allocation.get('scheduleRules', []),
            'allocationStatus': allocation.get('allocationStatus')
        }

    return allocation_map
