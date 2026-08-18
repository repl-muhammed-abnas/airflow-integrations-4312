"""
Request payload builders for the Replicon API.
"""

import uuid
null = None

def get_departments_payload():
    """
    Build payload for retrieving all departments from Replicon.
    
    Returns:
        Dictionary containing the API request payload
    """
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:full-path",
            "urn:replicon:department-group-list-column:full-path-code",
            "urn:replicon:department-group-list-column:effectively-enabled",
            "urn:replicon:department-group-list-column:description",
            "urn:replicon:department-group-list-column:name",
            "urn:replicon:department-group-list-column:code"
        ],
        "sort": [],
        "filterExpression": None
    }

def create_cost_center_payload(cost_center, parent_uri=None):
    """
    Build payload for creating a new cost center.
    
    Args:
        cost_center: Dictionary containing cost center data
        parent_uri: URI of the parent cost center or None for top level
        
    Returns:
        Dictionary containing the API request payload
    """
    # Generate unique ID for this operation
    unit_of_work_id = str(uuid.uuid4())
    
    # Get the actual cost center name (last part of pipe-separated path)
    name_parts = cost_center.get('Name', '').split('|')
    cost_center_name = name_parts[-1] if name_parts else ''
    
    # Parse status value (True/False)
    is_enabled = cost_center.get('Status', '').lower() in ['enabled']
    
    return {
        "departmentGroup": {
            "uri": None,
            "parent": {
                "uri": parent_uri,
                "parent": None,
                "name": None,
                "parameterCorrelationId": None
            } if parent_uri else None,
            "name": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "name": cost_center_name,
            "codeToApply": {
                "value": cost_center.get('Code', '')
            },
            "descriptionToApply": {
                "value": cost_center.get('Description', '')
            },
            "isEnabled": is_enabled
        },
        "unitOfWorkId": unit_of_work_id
    }

def update_cost_center_payload(cost_center, cost_center_uri):
    """
    Build payload for updating an existing cost center.
    
    Args:
        cost_center: Dictionary containing cost center data
        cost_center_uri: URI of the cost center to update
        
    Returns:
        Dictionary containing the API request payload
    """
    # Generate unique ID for this operation
    unit_of_work_id = str(uuid.uuid4())
    
    # Get the actual cost center name (last part of pipe-separated path)
    name_parts = cost_center.get('Name', '').split('|')
    cost_center_name = name_parts[-1] if name_parts else ''
    
    # Parse status value (True/False)
    is_enabled = cost_center.get('Status', '').lower() in ['enabled']
    
    return {
        "departmentGroup": {
            "uri": cost_center_uri,
            "parent": None,
            "name": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "name": cost_center_name,
            "codeToApply": {
                "value": cost_center.get('Code', '')
            },
            "descriptionToApply": {
                "value": cost_center.get('Description', '')
            },
            "isEnabled": is_enabled
        },
        "unitOfWorkId": unit_of_work_id
    }

def move_cost_center_payload(cost_center_uri, parent_uri):
    """
    Build payload for moving a cost center to a new parent.
    
    Args:
        cost_center_uri: URI of the cost center to move
        parent_uri: URI of the new parent cost center
        
    Returns:
        Dictionary containing the API request payload
    """
    return {
        "departmentGroup": {
            "uri": cost_center_uri,
            "parent": None,
            "name": None,
            "parameterCorrelationId": None
        },
        "target": {
            "uri": parent_uri,
            "parent": None,
            "name": None,
            "parameterCorrelationId": None
        }
    }

def get_all_permission_sets_payload():
    """
    Build payload for retrieving all permission sets.
    
    Returns:
        Dictionary containing the API request payload
    """
    return {}  # Empty payload for this endpoint

def assign_manager_permission_payload(user_uri, cost_manager_permission_uri, payroll_manager_permission_uri):
    """
    Build payload for assigning cost center manager permission to a user.
    
    Args:
        user_uri: URI of the user
        cost_manager_permission_uri: URI of the cost manager permission
        
    Returns:
        Dictionary containing the API request payload
    """
    return {
        "user": {
            "uri": user_uri
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": [
                    cost_manager_permission_uri,
                    payroll_manager_permission_uri
                ],
                "policyUrisToRemovePermissionSet": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def set_managed_cost_centers_payload(user_uri, cost_center_uris, instance):

    scope_uris = [
        "urn:replicon:object-type:project",
        "urn:replicon:object-type:user",
    ]

    policies = {
        "pm": "urn:replicon:policy:payroll-management",
        "cm": "urn:replicon:policy:cost-management",
    }

    department_groups = [
        {
            "departmentGroup": {
                "uri": uri,
                "parent": None,
                "name": None,
                "parameterCorrelationId": None,
            },
            "groupSpecificationModeUri": None,
            "groupDescendantModeUri": None,
        }
        for uri in cost_center_uris
    ]

    policy_data_access_scopes = []

    # Payroll Management
    policy_data_access_scopes.append(
        {
            "policyUri": policies["pm"],
            "departmentGroups": department_groups,
        }
    )

    # Cost Management
    for scope in scope_uris:
        payload = {
            "policyUri": policies["cm"],
            "departmentGroups": department_groups
        }
        if scope:
            payload["scopeObjectTypeUri"] = scope

        policy_data_access_scopes.append(payload)

    return {
        "userUri": user_uri,
        "policyDataAccessScopes": policy_data_access_scopes,
    }
