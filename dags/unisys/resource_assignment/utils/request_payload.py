"""
Unisys Resource Assignment - Request Payload Utilities
Generates API payloads for resource assignment operations (Polaris GraphQL)
"""
from datetime import datetime
import uuid
import rail


# ========== LOG PROPERTIES ==========

def get_project_not_found_log_properties(item):
    """Log properties when project is not found"""
    return {
        "workernumber": item.get('workernumber', ''),
        "projectnumber": item.get('projectnumber', ''),
        "action": "Validation",
        "status": "Exception",
        "details": f"Project {item.get('projectnumber')} does not exist in Replicon"
    }


def get_project_inactive_log_properties(item):
    """Log properties when project is inactive"""
    return {
        "workernumber": item.get('workernumber', ''),
        "projectnumber": item.get('projectnumber', ''),
        "action": "Validation",
        "status": "Exception",
        "details": f"Project {item.get('projectnumber')} is not active in Replicon"
    }


def get_error_log_properties(item):
    """Log properties for general errors"""
    try:
        return {
            "workernumber": item.get('workernumber', ''),
            "projectnumber": item.get('projectnumber', ''),
            "action": "Error",
            "status": "Error",
            "details": "{{ get_error_message() }}"
        }
    except:
        return {
            "workernumber": "",
            "projectnumber": "",
            "action": "Error",
            "status": "Error",
            "details": "{{ get_error_message() }}"
        }


# ========== GRAPHQL QUERIES ==========

def get_resource_allocations_graphql_query():
    """
    Get existing resource allocations for project using GraphQL query

    Returns allocation IDs, user URIs, start/end dates for all resources
    This is more efficient than REST API and gives us allocation IDs for updates

    Returns:
        dict: GraphQL query with variables
    """
    project_details = rail.result("get_project_details")

    return {
        "operationName": "ResourceAllocationsQuery",
        "variables": {
            "projectUri": project_details['uri']
        },
        "query": """query ResourceAllocationsQuery($projectUri: String!) {
  resourceAllocations(projectUri: $projectUri) {
    resourceAllocations {
      id
      projectUri
      user {
        userUri
        user {
          slug
          displayText
          uri
          __typename
        }
        __typename
      }
      allocationStatus
      startDate
      endDate
      scheduleRules {
        dateRange {
          startDate
          endDate
          __typename
        }
        do
        __typename
      }
      __typename
    }
    nextPageCursor
    __typename
  }
}"""
    }


# ========== GRAPHQL MUTATIONS ==========

def create_resource_allocation_mutation(item, DATE_FORMAT_INPUT):
    """
    Create NEW resource allocation using Polaris GraphQL mutation
    Uses: CreateFulfilledResourceAllocationWithoutResourceRequest

    Args:
        item: Resource item from CSV with user_uri, assignmentstartdate, assignmentenddate
        DATE_FORMAT_INPUT: Date format string (e.g., "%d-%b-%Y")

    Returns:
        dict: GraphQL create mutation with variables
    """
    project_details = rail.result("get_project_details")

    user_uri = item['user_uri']
    csv_start_date_str = item.get('assignmentstartdate', '').strip().upper()
    csv_end_date_str = item.get('assignmentenddate', '').strip().upper()

    csv_start_date = datetime.strptime(csv_start_date_str, DATE_FORMAT_INPUT)
    csv_end_date = datetime.strptime(csv_end_date_str, DATE_FORMAT_INPUT)

    # Generate new allocation ID
    allocation_id = f"urn:replicon-tenant:{project_details['uri'].split(':')[2]}:psa-resource-allocation:{str(uuid.uuid4())}"
    start_date_iso = csv_start_date.strftime('%Y-%m-%dT00:00:00.000Z')
    end_date_iso = csv_end_date.strftime('%Y-%m-%dT00:00:00.000Z')

    return {
        "operationName": "CreateFulfilledResourceAllocationWithoutResourceRequest",
        "variables": {
            "input": {
                "id": allocation_id,
                "projectUri": project_details['uri'],
                "scheduleRules": [
                    {
                        "dateRange": {
                            "startDate": start_date_iso,
                            "endDate": end_date_iso
                        },
                        "do": {
                            "load": 100,
                            "setHours": 8,
                            "excludeWeekdays": ["sa", "su"]
                        }
                    }
                ],
                "load": 100,
                "user": {
                    "userUri": user_uri,
                    "costRate": 0,
                    "costCurrencyUri": None,
                    "userType": "ASSIGNED"
                },
                "requestedRoleUri": None
            }
        },
        "query": """mutation CreateFulfilledResourceAllocationWithoutResourceRequest($input: CreateFulfilledResourceAllocationWithoutResourceRequestInput!) {
  createFulfilledResourceAllocationWithoutResourceRequest(input: $input) {
    resourceAllocation {
      id
      projectUri
      user {
        userUri
        user {
          slug
          displayText
          uri
          __typename
        }
        __typename
      }
      allocationStatus
      startDate
      endDate
      load
      __typename
    }
    __typename
  }
}"""
    }


def update_resource_allocation_mutation(item, DATE_FORMAT_INPUT):
    """
    Update EXISTING resource allocation using Polaris GraphQL mutation
    Uses: UpdateResourceAllocation (updateResourceAllocation2)

    CRITICAL: This prevents creating duplicate allocations
    CRITICAL: When start date differs, existing start date is retained

    Args:
        item: Resource item from CSV with user_uri, assignmentstartdate, assignmentenddate, existing_start_date, existing_allocation_id
        DATE_FORMAT_INPUT: Date format string (e.g., "%d-%b-%Y")

    Returns:
        dict: GraphQL update mutation with variables
    """
    project_details = rail.result("get_project_details")

    user_uri = item['user_uri']
    csv_start_date_str = item.get('assignmentstartdate', '').strip().upper()
    csv_end_date_str = item.get('assignmentenddate', '').strip().upper()

    csv_start_date = datetime.strptime(csv_start_date_str, DATE_FORMAT_INPUT)
    csv_end_date = datetime.strptime(csv_end_date_str, DATE_FORMAT_INPUT)

    # Get existing allocation details from the item (stored during prepare_resource_processing)
    allocation_id = item.get('existing_allocation_id', '')
    existing_start = item.get('existing_start_date', '')

    # Convert existing start date to datetime for comparison
    if existing_start:
        existing_start_dt = datetime.fromisoformat(existing_start.replace('Z', '+00:00'))
        csv_start_dt = csv_start_date.replace(tzinfo=existing_start_dt.tzinfo)

        if existing_start_dt.date() != csv_start_dt.date():
            # Start date mismatch - preserve existing start date
            start_date_iso = existing_start
        else:
            # Start dates match - use CSV start date
            start_date_iso = csv_start_date.strftime('%Y-%m-%dT00:00:00.000Z')
    else:
        start_date_iso = csv_start_date.strftime('%Y-%m-%dT00:00:00.000Z')

    end_date_iso = csv_end_date.strftime('%Y-%m-%dT00:00:00.000Z')

    return {
        "operationName": "UpdateResourceAllocation",
        "variables": {
            "input": {
                "user": {
                    "userUri": user_uri,
                    "userType": "ASSIGNED",
                    "costRate": 0,
                    "costCurrencyUri": None
                },
                "id": allocation_id,
                "scheduleRules": [
                    {
                        "dateRange": {
                            "startDate": start_date_iso,
                            "endDate": end_date_iso
                        },
                        "do": {
                            "load": 100,
                            "setHours": 8,
                            "excludeWeekdays": ["sa", "su"]
                        }
                    }
                ],
                "load": 100,
                "isAdjustedLoading": None,
                "requestedRoleUri": None
            },
            "showTimeOff": False,
            "showHolidays": False,
            "requestStatusList": ["DRAFT", "REJECTED", "SUBMITTED", "TOBEHIRED"],
            "allocationStatusList": ["COMMITTED"],
            "filter": {}
        },
        "query": """mutation UpdateResourceAllocation($input: UpdateResourceAllocationInput!) {
  updateResourceAllocation2(input: $input) {
    resourceAllocation {
      id
      projectUri
      user {
        userUri
        user {
          slug
          displayText
          uri
          __typename
        }
        __typename
      }
      allocationStatus
      startDate
      endDate
      load
      __typename
    }
    __typename
  }
}"""
    }


# ========== TASK ASSIGNMENT (REST API) ==========

def bulk_assign_all_resources_to_task(item):
    """
    Assign ALL resources to a single task using BulkUpdateResourceAssignments

    This is the most efficient approach:
    - ForEach over tasks (not resources)
    - For each task, assign ALL resources at once

    Args:
        item: Task item with uri

    Returns:
        dict: Payload for BulkUpdateResourceAssignments API
    """
    all_records = rail.result("load_all_assignment_records")

    # Get all resource URIs
    resource_uris = [record['user_uri'] for record in all_records]

    return {
        "taskUri": item['uri'],
        "resourceUris": resource_uris,
        "isAssigned": True
    }
