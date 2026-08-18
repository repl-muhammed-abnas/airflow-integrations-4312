"""
WCG Project Sync - Request Methods
Converted from Workato Integration - January 2026

This module contains Replicon API request builders for:
- Project creation and updates
- Client creation
- Custom field management
- Subsidiary dropdown management

Based on Workato recipes:
- live_wcg_netsuite_project_sync_v2_0.recipe.json
- live_wcg_update_subsidiary_value_on_project.recipe.json
- live_wcg_update_subsidiary_value_at_system_level.recipe.json
"""

from uuid import uuid4
import rail
from wcg.project_sync.utils.custom_methods import parse_date_safe

null = None


def update_project_request(dag_run):
    """
    Build request payload for updating an existing project in Replicon.
    Uses ProjectService1.svc/CreateProjectOrApplyModifications

    Matches Workato Step 151: update_project (for existing projects)
    Updates: End Date, P&L Type, Department, Subsidiary
    Note: PM is handled separately in steps 168/170/171
    Note: Budget is handled separately in step 157

    Args:
        dag_run: Airflow DAG run context containing project data

    Returns:
        Tuple of (request_payload, update_log_message)
    """
    conf = dag_run.conf
    custom_field_uris = conf.get("custom_field_uris", {})

    # Get project URI from step_36_search_project_by_code (like Workato does)
    search_result = rail.result("step_36_search_project_by_code")
    project_uri = search_result.get("uri") if search_result else null

    if not project_uri:
        return null, "No existing project found"

    logs = ""

    # Parse end date (from Workato: end_date)
    end_date = parse_date_safe(conf.get("end_date"))
    if end_date:
        logs += "End Date; "

    # Build custom fields list
    custom_fields = []

    # P&L Type (dropdown) - from Workato: custom_p_l_type
    pl_drop_uri = conf.get("pl_drop_uri")
    if pl_drop_uri and custom_field_uris.get("pl_type_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["pl_type_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": pl_drop_uri,
                "name": null,
            },
        })
        logs += "P&L Type; "
    elif conf.get("pl_type") and custom_field_uris.get("pl_type_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["pl_type_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": null,
                "name": conf["pl_type"],
            },
        })
        logs += "P&L Type; "

    # Department (dropdown) - from Workato: custom_project_department
    # Workato always passes the value, even if empty
    department_drop_uri = conf.get("department_drop_uri")
    if department_drop_uri and custom_field_uris.get("department_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["department_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": department_drop_uri,
                "name": null,
            },
        })
        logs += "Department; "
    elif conf.get("department") and custom_field_uris.get("department_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["department_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": null,
                "name": conf["department"],
            },
        })
        logs += "Department; "
    # Note: Department is a REQUIRED field - do not send null when empty

    # Subsidiary (dropdown) - from Workato: custom_project_subsidiary
    subsidiary_drop_uri = conf.get("subsidiary_drop_uri")
    if subsidiary_drop_uri and custom_field_uris.get("project_subsidiary_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["project_subsidiary_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": subsidiary_drop_uri,
                "name": null,
            },
        })
        logs += "Subsidiary; "
    elif conf.get("subsidiary") and custom_field_uris.get("project_subsidiary_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["project_subsidiary_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": null,
                "name": conf["subsidiary"],
            },
        })
        logs += "Subsidiary; "

    return {
        "target": {
            "uri": project_uri,
        },
        "modifications": {
            "endDateToApply": {
                "date": end_date,
            } if end_date else null,
            "customFieldsToApply": custom_fields if custom_fields else null,
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4()),
    }, logs


def create_client_request(dag_run):
    """
    Build request payload for creating a new client in Replicon.
    Uses ClientService1.svc/PutClient

    Args:
        dag_run: Airflow DAG run context

    Returns:
        Dictionary with API request payload
    """
    client_name = dag_run.conf.get("customer", "")

    return {
        "client": {
            "target": {
                "uri": null,
                "name": client_name,
                "code": null,
                "parameterCorrelationId": null
            },
            "name": client_name,
            "code": str(dag_run.conf.get("customer_internal_id", "")),
            "comment": null,
            "clientManager": null,
            "billingContact": null,
            "clientAddress": null,
            "billingAddress": null,
            "isActive": "true",
            "customFieldValues": [],
            "billingRates": [],
            "expenseCodesAllowedByDefaultOnNewProjects": [],
            "defaultBillingCurrency": null
        }
    }


def get_search_user_request(user_name):
    """
    Build request to search for user (project manager) by name.
    Uses UserlistService1.svc/GetData

    Args:
        user_name: Project manager name from feed file

    Returns:
        Dictionary with API request payload
    """
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": ["urn:replicon:user-list-column:user"],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text",
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {"text": user_name},
            },
        },
    }


def get_search_client_by_code_request(client_code):
    """
    Build request to search for existing client by code (customer internal id).
    Uses ClientListService1.svc/GetData

    Based on Workato logic: Search client based on code (internal_id)

    Args:
        client_code: Customer internal ID from feed file

    Returns:
        Dictionary with API request payload
    """
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code",
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code",
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {"text": str(client_code)},
            },
        },
    }


def create_project_copy_batch_request(dag_run, template_project_uri, client_name):
    """
    Build request to create a new project by copying from a template.
    Uses ProjectService1.svc/CreateProjectCopyBatch2

    Based on Workato recipe: live_wcg_netsuite_project_sync_v2_0.recipe.json

    Args:
        dag_run: Airflow DAG run context
        template_project_uri: URI of the template project to copy from
        client_name: Name of the client to assign

    Returns:
        Dictionary with API request payload
    """
    conf = dag_run.conf

    # Parse start date
    start_date = parse_date_safe(conf.get("start_date"))

    # Get formatted project name from Step 29 (format: "{name_part} - {internal_id}")
    project_name = rail.result("step_29_log_start_processing")

    return {
        "copyParameter": {
            "sourceProject": {
                "uri": template_project_uri,
                "name": null,
                "parameterCorrelationId": null,
            },
            "destinationProjectInfo": {
                "name": project_name,
                "dateRange": {
                    "startDate": start_date if start_date else {
                        "year": 2026,
                        "month": 1,
                        "day": 1,
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null,
                },
                "statusLabel": {
                    "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:project-status-label:62776869-c1d9-405e-8352-98c9e50d5795",
                    "name": null,
                },
                "clients": [
                    {
                        "client": {
                            "uri": null,
                            "name": client_name,
                        },
                        "costAllocationPercentage": "100",
                    }
                ] if client_name else [],
                "program": null,
            },
            "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
            "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:copy",
            "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:copy-from-project",
            "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:copy-from-project",
            "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:shift-by-project-start-date-offset",
        }
    }


def update_project_code_request(project_uri, code):
    """
    Build request to update project code.
    Uses ProjectService1.svc/UpdateCode

    Based on Workato recipe: Updates project code after copy creation

    Args:
        project_uri: URI of the project to update
        code: New code value (internal_id from feed file)

    Returns:
        Dictionary with API request payload
    """
    return {
        "projectUri": project_uri,
        "code": str(code),
    }


def update_project_leader_request(project_uri, user_uri):
    """
    Build request to update project leader/manager.
    Uses ProjectService1.svc/UpdateProjectLeader

    Based on Workato recipe: Updates project leader after creation

    Args:
        project_uri: URI of the project
        user_uri: URI of the user to set as leader

    Returns:
        Dictionary with API request payload
    """
    return {
        "projectUri": project_uri,
        "userUri": user_uri,
    }


def update_project_custom_fields_request(dag_run, project_uri):
    """
    Build request to update project with custom fields after copy.
    Uses ProjectService1.svc/CreateProjectOrApplyModifications

    Matches Workato Step 132: Update project in Replicon
    Updates: End Date, P&L Type, Department
    Note: Subsidiary is updated separately in Step 131 via UpdateDropdownValue

    Args:
        dag_run: Airflow DAG run context
        project_uri: URI of the project to update

    Returns:
        Dictionary with API request payload
    """
    conf = dag_run.conf
    custom_field_uris = conf.get("custom_field_uris", {})

    # Parse end date
    end_date = parse_date_safe(conf.get("end_date"))

    # Build custom fields list
    custom_fields = []

    # Note: Project Subsidiary is handled separately in Step 131 via UpdateDropdownValue

    # P&L Type (dropdown) - from Workato step 132 custom_p_l_type
    # Uses pre-resolved URI from master DAG (pl_drop_uri)
    pl_drop_uri = conf.get("pl_drop_uri")
    if pl_drop_uri and custom_field_uris.get("pl_type_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["pl_type_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": pl_drop_uri,
                "name": null,
            },
        })
    elif conf.get("pl_type") and custom_field_uris.get("pl_type_uri"):
        # Fallback: use name-based lookup if URI not pre-resolved
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["pl_type_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": null,
                "name": conf["pl_type"],
            },
        })

    # Department (dropdown) - from Workato step 132 custom_project_department
    # Uses pre-resolved URI from master DAG (department_drop_uri)
    # If department is empty in CSV, explicitly clear it (don't inherit from template)
    department_drop_uri = conf.get("department_drop_uri")
    if department_drop_uri and custom_field_uris.get("department_uri"):
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["department_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": department_drop_uri,
                "name": null,
            },
        })
    elif conf.get("department") and custom_field_uris.get("department_uri"):
        # Fallback: use name-based lookup if URI not pre-resolved
        custom_fields.append({
            "customField": {
                "uri": custom_field_uris["department_uri"],
                "name": null,
                "groupUri": "urn:replicon:object-type:project",
            },
            "dropDownOption": {
                "uri": null,
                "name": conf["department"],
            },
        })
    # Note: Department is a REQUIRED field - do not send null when empty

    return {
        "target": {
            "uri": project_uri,
        },
        "modifications": {
            "endDateToApply": {
                "date": end_date,
            } if end_date else null,
            "customFieldsToApply": custom_fields if custom_fields else null,
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4()),
    }


def get_search_project_by_code_request(project_code):
    """
    Build request to search for existing project by code (internal ID).
    Uses ProjectListService1.svc/GetData

    Based on Workato recipe: "Search project based on code (internal id)"

    Args:
        project_code: NetSuite internal ID used as project code

    Returns:
        Dictionary with API request payload
    """
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code",
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:project-list-filter:code",
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {"text": str(project_code)},
            },
        },
    }


def get_template_project_search_request(template_name):
    """
    Build request to search for template project by name.
    Uses ProjectListService1.svc/GetData

    Based on WCG_Project_Mapper lookup: Finds template project URI by name

    Args:
        template_name: Name of the template project from mapper

    Returns:
        Dictionary with API request payload
    """
    return {
        "page": "1",
        "pagesize": "10",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code",
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:project-list-filter:name",
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {"text": template_name},
            },
        },
    }


def update_total_estimated_contract_value_request(project_uri, amount, currency_uri=None):
    """
    Build request to update project's total estimated contract value (budget).
    Uses ProjectService1.svc/UpdateTotalEstimatedContractValue

    Based on Workato recipe: Updates project budget after creation/copy

    Args:
        project_uri: URI of the project to update
        amount: Budget amount (numeric)
        currency_uri: Optional currency URI (defaults to WCG tenant currency)

    Returns:
        Dictionary with API request payload
    """
    # Default WCG currency URI if not specified
    if currency_uri is None:
        currency_uri = f"urn:replicon-tenant:{rail.get_tenant_slug()}:currency:1"

    return {
        "projectUri": project_uri,
        "totalEstimatedContract": {
            "amount": str(amount),
            "currencyUri": currency_uri,
        }
    }


def update_time_entry_date_range_request(project_uri, start_date, end_date=None):
    """
    Build request to update project's time entry date range (start/end dates).
    Uses ProjectService1.svc/UpdateTimeEntryDateRange

    Based on Workato recipe: Updates project start date after creation

    Args:
        project_uri: URI of the project to update
        start_date: Start date dict with year, month, day
        end_date: Optional end date dict with year, month, day

    Returns:
        Dictionary with API request payload
    """
    return {
        "projectUri": project_uri,
        "timeEntryDateRange": {
            "startDate": start_date,
            "endDate": end_date,
        }
    }


def get_user_permission_sets_v2_request(user_uri):
    """
    Build request to get user's assigned permission sets (v2 endpoint).
    Uses PermissionSetService1.svc/GetAssignedPermissionSetsForUser2

    This is the newer version used in the Workato recipe, different from
    UserService1.svc/GetPermissionSetsAssignedToUser

    Args:
        user_uri: URI of the user

    Returns:
        Dictionary with API request payload
    """
    return {
        "userUri": user_uri,
    }


def assign_permission_set_to_user_v2_request(user_uri, permission_set_uri):
    """
    Build request to assign a permission set to a user.
    Uses PermissionSetService1.svc/AssignPermissionSetToUser

    Based on Workato recipe: Assigns project management permission to user

    Args:
        user_uri: URI of the user
        permission_set_uri: URI of the permission set to assign

    Returns:
        Dictionary with API request payload
    """
    return {
        "userUri": user_uri,
        "permissionSetUri": permission_set_uri,
    }
