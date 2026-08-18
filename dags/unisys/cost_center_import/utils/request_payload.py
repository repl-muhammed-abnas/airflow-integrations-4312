"""
Request Payload Module for Unisys Cost Center Import Integration

This module contains functions to generate request payloads for Replicon API calls
based on the design document specifications.

Functions:
    get_hierarchy_data_payload: Generate payload for GetHierarchyData API call
    create_cost_center_payload: Generate payload for creating new cost centers
    update_cost_center_payload: Generate payload for updating existing cost centers
    disable_cost_center_payload: Generate payload for disabling cost centers

Design Reference:
    Based on cost_center_design.txt specifications for Division service calls
"""
import uuid

null=None

def get_hierarchy_data_payload():
    """
    Generate payload for GetHierarchyData service call to retrieve divisions.

    Design Reference (from cost_center_design.txt):
        Service: /services/DivisionListService1.svc/GetHierarchyData
        Request structure with pagination and column URIs

    Args:
        page_number (int): Page number for pagination (default: 1)

    Returns:
        dict: Request payload for GetHierarchyData API

    Example:
        >>> payload = get_hierarchy_data_payload(1)
        >>> # Returns payload for first page with 1000 records
    """
    return {
        "page": 1,
        "pagesize": 1000,
        "columnUris": [
            "urn:replicon:division-list-column:full-path",
            "urn:replicon:division-list-column:full-path-code",
            "urn:replicon:division-list-column:effectively-enabled",
        ],
        "filterExpression": null,
        "hierarchyListDataOptionUris": [],
    }


def create_cost_center_payload(
    company_code, cost_center_code, cost_center_name, status
):
    """
    Generate payload for creating a new cost center under a company.

    Design Reference (from cost_center_design.txt - Step 13):
        Creates cost center hierarchy under parent company
        Target: company parent (identified by name)
        ModificationToApply: cost center with code and name

    Args:
        company_code (str): Company code (parent identifier)
        company_name (str): Company name (parent name)
        cost_center_code (str): Cost center code to create
        cost_center_name (str): Cost center name to create

    Returns:
        dict: Request payload for CreateDivisionHierarchyOrApplyModifications API

    Example:
        >>> payload = create_cost_center_payload("101", "UNISYS CORPORATION", "1005", "DWS GTM Sales")
    """


    return {
        "division": {
            "name": null,
            "uri": null,
            "parent": {
            "name": company_code,
            "uri": null,
            "parent": null,
            "parameterCorrelationId": null
            },
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": cost_center_code,
            "codeToApply": {"value": cost_center_name[:50]},
            "descriptionToApply": null,
            "isEnabled": "true" if status.lower() == "enabled" else "false"
        },
        "unitOfWorkId": str(uuid.uuid4()),
        }


def update_cost_center_payload(company_code, cost_center_code, cost_center_name, status):
    """
    Generate payload for updating an existing cost center's name.

    Design Reference (from cost_center_design.txt - Step 14):
        Updates cost center when name has changed
        Target: identifies existing cost center by parent and name
        ModificationToApply: updates the codeToApply (display name)

    Args:
        company_code (str): Company code (parent identifier)
        cost_center_code (str): Cost center code to update
        cost_center_name (str): New cost center name

    Returns:
        dict: Request payload for CreateDivisionHierarchyOrApplyModifications API

    Example:
        >>> payload = update_cost_center_payload("101", "1005", "DWS GTM Sales Updated")
    """

    return {
        "division": {
            "name": cost_center_code,
            "uri": null,
            "parent": {
            "name": company_code,
            "uri": null,
            "parent": null,
            "parameterCorrelationId": null
            },
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": cost_center_code,
            "codeToApply": {"value": cost_center_name[:50]},
            "descriptionToApply": null,
            "isEnabled": "true" if status.lower() == "enabled" else "false"
        },
        "unitOfWorkId": str(uuid.uuid4()),
        }