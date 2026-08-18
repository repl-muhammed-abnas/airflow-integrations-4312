"""
Request payload generation functions for CostPoint project sync.
Contains functions that generate API request payloads for various endpoints.
"""
from capefoxcorporation.project_sync.utils import custom_methods
from capefoxcorporation.project_sync.utils.custom_methods import (
    _build_assigned_resource_dict,
    _build_closed_task_def
)

import rail

null = None

def build_custom_field_values(dag_run, root_project_info, config=null):
    """Build custom field values, filtering out any with null URIs"""
    custom_fields = []
    
    # Define custom field mappings - use config if provided, otherwise hardcoded values
    field_mappings = []
    
    # Use config field names if available
    if config:
        field_mappings = [
            (config.proj_purchase_order_no, root_project_info.get('CUST_PO_ID')),
            (config.proj_project_classification, root_project_info.get('S_PROJ_RPT_DC')),
            (config.proj_user_company, dag_run.conf['item']['data'][0].get('_company')),
            (config.proj_opportunity_id, root_project_info.get('OPP_ID'))
        ]
    else:
        # Fallback to hardcoded values
        field_mappings = [
            ('Purchase Order No', root_project_info.get('CUST_PO_ID')),
            ('Project Classification', root_project_info.get('S_PROJ_RPT_DC')),
            ('Company', dag_run.conf['item']['data'][0].get('_company')),
            ('Opportunity ID', root_project_info.get('OPP_ID'))
        ]
    
    for field_name, field_value in field_mappings:
        if field_value:  # Only add if value exists
            uri = rail.find_first_by_attr_and_get_attr(
                dag_run.conf.get('project_udfs', []), 
                'textValue', 
                field_name, 
                'uri'
            )
            if uri:  # Only add if URI is found
                custom_fields.append({
                    "customField": {"uri": uri},
                    "text": field_value
                })
    
    return custom_fields


def validate_and_clean_payload(project_payload):
    """Remove any null URIs and empty arrays from the payload before sending"""
    # Clean custom field values - remove any with null URIs
    if "projectInfo" in project_payload["project"]:
        if "customFieldValues" in project_payload["project"]["projectInfo"]:
            project_payload["project"]["projectInfo"]["customFieldValues"] = [
                cf for cf in project_payload["project"]["projectInfo"]["customFieldValues"] 
                if cf.get("customField", {}).get("uri")
            ]
    
    return project_payload

def get_costpoint_projects_payload(dag_run):
    """Generate payload for CostPoint projects API call"""
    return {
        "filter": {
            "id": "replicon_exp_project",
            "where": [
                {
                    "rsWhere": {
                        "rsId": "PJMBASIC_PROJ",
                        "conditions": [
                            {
                                "joinWithParent": "N",
                                "relations": [
                                    {
                                        "name": "PROJ_ID",
                                        "relation": "like%",
                                        "value": dag_run.conf['item']['root_project_id']
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
    }


def get_workforce_user_costpoint_payload(dag_run):
    """Generate payload for CostPoint workforce users API call"""
    return {
        "filter": {
            "id": "replicon_exp_project_workforce",
            "where": [
                {
                    "rsWhere": {
                        "rsId": "PJM_PROJEMPL_HDR",
                        "conditions": [
                            {
                                "joinWithParent": "N",
                                "relations": [
                                    {
                                        "name": "PROJ_ID",
                                        "relation": "like%",
                                        "value": custom_methods.get_project_data(dag_run)[0]
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
    }


def get_bulk_users_payload():
    """Generate payload for Replicon bulk users API call"""
    return {
        "users": list(map(lambda x: {"employeeId": x}, custom_methods.map_workforce_empid()))
    }


def get_project_leader_users_payload(dag_run):
    """Generate payload for project leader user API call"""
    return {
        "users": [{"employeeId": custom_methods.get_project_data(dag_run)[2].get('EMPL_ID')}]
    }

def get_project_details_payload(dag_run):
    """Generate payload for Replicon project details API call"""
    return {
        "projects": [
            {
                "uri": null,
                "name": null,
                "code": dag_run.conf['item']['root_project_id'],
                "parameterCorrelationId": null
            }
        ]
    }


def get_assign_permission_payload(dag_run):
    """Generate payload for assigning project manager permission"""
    return {
        "userUri": rail.result('get_project_leader_info_from_replicon')['uri'],
        "permissionSetUri": rail.find_first_by_attr_and_get_attr(
            dag_run.conf['permission_sets'], 
            'name', 
            dag_run.conf['project_manager_permission_name'], 
            'uri'
        )
    }


def get_update_task_name_payload(item):
    """Generate payload for updating task name"""
    return {
        "taskUri": item['uri'],
        "name": item['new_name']
    }


def get_update_division_payload(dag_run):
    """Generate payload for updating project division"""
    return {
        "projectUri": (rail.result('add_project_and_task') or {}).get('uri') or (rail.result('get_project_details')['project'] or {}).get('uri'),
        "division": {
            "name": rail.find_first_by_attr_and_get_attr(
                dag_run.conf['divisions'],
                'code',
                custom_methods.get_project_data(dag_run)[2]['ORG_ID'],
                'name'
            )
        }
    }


def get_add_project_and_task_param(dag_run, date_time_format, config=null):
    """Generate complete project and task creation payload"""

    root_project_id, data, root_project_info = custom_methods.get_project_data(dag_run)

    payload = {
        "project": {
            "target": {
                "uri": null,
                "name": null,
                "code": root_project_info['PROJ_ID'],
                "parameterCorrelationId": null
            },
            "projectInfo": {
                "name": root_project_info['PROJ_NAME'],
                "code": root_project_info['PROJ_ID'],
                "description": root_project_info['PROJ_LONG_NAME'],
                "timeEntryDateRange": {
                    "startDate": rail.parse_date(root_project_info.get('PROJ_START_DT'), date_time_format),
                    "endDate": rail.parse_date(root_project_info.get('PROJ_END_DT'), date_time_format),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "projectStatusLabel": {
                    "uri": null,
                    "name": "In Progress" if root_project_info.get('ACTIVE_FL') == 'Y' else "Completed"
                },
                "percentCompleted": "0",
                "client": {
                    "uri": null,
                    "name": root_project_info['CUST_NAME'],
                    "code": null,
                    "parameterCorrelationId": null
                } if root_project_info.get('CUST_NAME') else null,
                "program": null,
                "projectLeader": {
                    "uri": null,
                    "loginName": null,
                    "employeeId": root_project_info.get('EMPL_ID'),
                    "parameterCorrelationId": null
                } if rail.result('get_project_leader_info_from_replicon') else null,
                "customFieldValues": build_custom_field_values(dag_run, root_project_info, config),
                "isTimeEntryAllowed": "false",
                "costTypeUri": null,
                "estimatedHours": null,
                "estimatedCost": null,
                "estimatedExpenses": null,
                "budget": null,
                "isProjectLeaderApprovalRequired": "true",
                "estimationModeUri": "urn:replicon:project-estimation-mode:task-based",
                "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                "timeAndMaterials": {
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:non-billable",
                    "billingRateFrequency": null,
                    "billingRateFrequencyDuration": null,
                    "billingRates": []
                },
                "defaultBillingCurrency": null
            },
            "tasks": get_tasks_param(dag_run, data, root_project_id, 1, date_time_format),
            "team": {
                "teamMembers": get_project_team_members(dag_run)
            },
            "expenses": null,
            "timeAndMaterials": {
                "billingRates": get_billing_rates_param(dag_run),
                "userSpecificBillingRates": []
            },
            "fixedBid": null
        }
    }

    # Validate and clean the payload before returning
    return validate_and_clean_payload(payload)


def get_tasks_param(dag_run, data, parent_id, level_no, date_time_format):
    """Generate task parameters for project hierarchy.

    For PUT calls, we must always include both old and new tasks:
    - New/updated tasks from Costpoint data (preserving Replicon values where applicable)
    - Orphan tasks (in Replicon but not in Costpoint) marked as closed
    """

    # Get tasks for current level (valid children for this parent in Costpoint)
    current_level_tasks = list(filter(
        lambda x: x['LVL_NO'] == level_no+1 and x['PROJ_ID'].startswith(parent_id), data))

    # Valid children codes for THIS parent level
    valid_children_codes = {x['PROJ_ID'] for x in current_level_tasks}

    # Get parent's Replicon URI for looking up existing children
    parent_uri = custom_methods.get_replicon_uri_by_code(parent_id)

    result_tasks = []
    for x in current_level_tasks:
        # Check if this is a bottom-level task (no children in Costpoint)
        has_children = any(
            child['LVL_NO'] == x['LVL_NO']+1 and child['PROJ_ID'].startswith(x['PROJ_ID'])
            for child in data
        )

        # Task naming logic as per spec:
        # If Allow Charging = FALSE, Task Name = "/"
        # Otherwise use project name (handle duplicates by appending ID)
        if x.get('ALLOW_CHARGES_FL') == 'N':
            task_name = "/"
        else:
            task_name = x['PROJ_NAME'] if len(list(filter(lambda p: p['PROJ_NAME'] == x['PROJ_NAME'], data))) == 1 else f"{x['PROJ_NAME']}_{x['PROJ_ID']}"

        # Check if this task already exists in Replicon - get full details for preserving values
        existing_task = custom_methods.get_existing_task_by_code(x['PROJ_ID'])

        # Build target structure based on whether task exists
        if existing_task:
            target = {
                "uri": existing_task['uri'],
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            }
        else:
            target = {
                "uri": null,
                "name": task_name,
                "parent": null,
                "parameterCorrelationId": null
            }

        task_def = {
            "task": {
                "target": target,
                "name": task_name,
                "code": x['PROJ_ID'],
                "description": x['PROJ_LONG_NAME'],
                "timeEntryDateRange": existing_task['timeEntryDateRange'] if existing_task else null,
                "percentCompleted": existing_task['percentCompleted'] if existing_task else "0",
                "isTimeEntryAllowed": "false",
                "estimatedHours": existing_task['estimatedHours'] if existing_task else null,
                "isClosed": "true" if x.get('ACTIVE_FL') == 'N' else "false",
                "customFieldValues": existing_task['customFieldValues'] if existing_task else [],
                "extensionFieldValues": existing_task['extensionFieldValues'] if existing_task else [],
                "estimatedCost": existing_task['estimatedCost'] if existing_task else null,
                "costTypeUri": existing_task['costTypeUri'] if existing_task else null,
                "assignedResources": custom_methods.get_assigned_resource_param_task(x, has_children),
                "timeAndMaterials": existing_task['timeAndMaterials'] if existing_task else null,
                "keyValues": existing_task['keyValues'] if existing_task else [],
                "historicalKeyValues": existing_task['historicalKeyValues'] if existing_task else []
            }
        }

        # Add child tasks recursively
        child_tasks = get_tasks_param(dag_run, data, x['PROJ_ID'], x['LVL_NO'], date_time_format)

        # Get PLCs assigned to this task (PLCs can be assigned at any level, not just bottom-level)
        plc_subtasks = get_plc_subtasks(dag_run, x, date_time_format)

        # Combine child tasks (from Costpoint hierarchy) with PLCs (from workforce data)
        # Both can exist at the same level
        task_def["childTasks"] = child_tasks + plc_subtasks

        result_tasks.append(task_def)

    # Add existing children from Replicon that are NOT in Costpoint and NOT PLCs
    # For PUT calls, we MUST include ALL existing children to prevent deletion
    # Note: PLCs are handled by get_plc_subtasks() - we only handle orphan TASKS here
    if parent_uri:
        existing_children = custom_methods.get_existing_children_for_parent(parent_uri)

        for existing in existing_children:
            # Skip if this is a valid Costpoint child task (already handled above)
            if existing['code'] in valid_children_codes:
                continue

            # Skip if this is a PLC (handled by get_plc_subtasks())
            # PLCs are identified by their code format - they don't follow the parent_id hierarchy
            # Costpoint task codes start with parent_id (e.g., 016180.001.00.0001 starts with 016180.001.00)
            # PLC codes don't (e.g., 01, 02, 0005AD don't start with 016180.001.00)
            if not existing['code'].startswith(parent_id):
                continue  # This is a PLC, handled by get_plc_subtasks()

            # Recursively get all children of this orphan task (they must also be included)
            orphan_children = custom_methods.get_orphan_children_recursive(dag_run, existing['uri'])

            # Orphan task - close it but preserve existing Replicon values
            result_tasks.append(_build_closed_task_def(existing, orphan_children))

    return result_tasks


def get_plc_subtasks(dag_run, parent_task, date_time_format):
    """Create PLC subtasks for a task.

    PLCs can be assigned at ANY level in Costpoint, not just bottom-level tasks.
    This function is called for every task to get its assigned PLCs.

    For PUT calls, we must always include both old and new PLCs:
    - New/updated PLCs from Costpoint workforce data (preserving Replicon values)
    - Orphan PLCs (in Replicon but not in workforce) marked as closed

    Returns:
        list: PLC task definitions (always returns a list, never None)
    """
    plc_tasks = []

    # Get the parent task's Replicon URI and existing children (once)
    parent_task_uri = custom_methods.get_replicon_uri_by_code(parent_task['PROJ_ID'])
    existing_children = custom_methods.get_existing_children_for_parent(parent_task_uri) if parent_task_uri else []

    # Build a lookup dict for existing PLCs by code for O(1) access
    existing_by_code = {child['code']: child for child in existing_children}

    # Get PLCs assigned to this project/task from workforce data
    assigned_plcs, _ = custom_methods.get_assigned_plcs_for_task(dag_run, parent_task['PROJ_ID'])
    assigned_plc_codes = {plc['code'] for plc in assigned_plcs}

    # Add PLCs from workforce data (new or updated)
    for plc in assigned_plcs:
        existing_plc = existing_by_code.get(plc['code'])

        # Build target structure based on whether PLC exists
        if existing_plc:
            target = {
                "uri": existing_plc['uri'],
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            }
        else:
            target = {
                "uri": null,
                "name": plc['name'],
                "parent": null,
                "parameterCorrelationId": null
            }

        plc_task = {
            "task": {
                "target": target,
                "name": plc['name'],
                "code": plc['code'],
                "description": f"PLC Task for {plc['name']}",
                "timeEntryDateRange": existing_plc.get('timeEntryDateRange') if existing_plc else null,
                "percentCompleted": existing_plc.get('percentCompleted', '0') if existing_plc else "0",
                "isTimeEntryAllowed": "true" if parent_task.get('ALLOW_CHARGES_FL') == 'Y' else "false",
                "estimatedHours": existing_plc.get('estimatedHours') if existing_plc else null,
                "isClosed": "true" if parent_task.get('ACTIVE_FL') == 'N' else "false",
                "customFieldValues": existing_plc.get('customFieldValues', []) if existing_plc else [],
                "extensionFieldValues": existing_plc.get('extensionFieldValues', []) if existing_plc else [],
                "estimatedCost": existing_plc.get('estimatedCost') if existing_plc else null,
                "costTypeUri": existing_plc.get('costTypeUri') if existing_plc else null,
                "assignedResources": custom_methods.get_plc_assigned_resources(parent_task['PROJ_ID'], plc['code']),
                "timeAndMaterials": existing_plc.get('timeAndMaterials') if existing_plc else null,
                "keyValues": existing_plc.get('keyValues', []) if existing_plc else [],
                "historicalKeyValues": existing_plc.get('historicalKeyValues', []) if existing_plc else []
            },
            "childTasks": []
        }
        plc_tasks.append(plc_task)

    # Add orphan PLCs - PLCs in Replicon but not assigned in Costpoint workforce
    # Note: Only handle actual PLCs here, NOT Costpoint child tasks
    for existing in existing_children:
        # Skip Costpoint child tasks - they're handled by get_tasks_param()
        # Costpoint task codes start with parent_id (e.g., 016180.001.00.0001 starts with 016180.001.00)
        # PLC codes don't follow this pattern (e.g., 01, 02, 0005AD)
        if existing['code'].startswith(parent_task['PROJ_ID']):
            continue

        if existing['code'] in assigned_plc_codes:
            continue  # Already handled above

        # Orphan PLC - close it but preserve existing Replicon values
        plc_tasks.append(_build_closed_task_def(existing))

    return plc_tasks


def get_billing_rates_param(dag_run):
    """Generate billing rates parameters for project"""
    
    rates = [
        {
            "billingRate": {
                "uri": "urn:replicon:project-specific-billing-rate",
                "name": null
            },
            "rateSchedule": null
        },
        {
            "billingRate": {
                "uri": "urn:replicon:user-specific-billing-rate",
                "name": null
            },
            "rateSchedule": null
        }
    ]
    
    return rates




def get_modified_projects_chunk_payload(item):
    """Generate payload for getting modified projects in chunks"""
    return {
        "filter": {
            "id": "replicon_exp_project",
            "where": [
                {
                    "rsWhere": {
                        "rsId": "PJMBASIC_PROJ",
                        "conditions": [
                            {
                                "joinWithParent": "N",
                                "relations": item
                            }
                        ]
                    }
                }
            ]
        }
    }


def get_modified_projects_payload(costpoint_time_zone):
    """Generate payload for getting all modified projects"""
    return {
        "filter": {
            "id": "replicon_exp_project",
            "where": [
                {
                    "rsWhere": {
                        "rsId": "PJMBASIC_PROJ",
                        "conditions": [
                            {
                                "joinWithParent": "N",
                                "relations": custom_methods.get_filters(costpoint_time_zone)
                            }
                        ]
                    }
                }
            ]
        }
    }


def get_all_clients_payload():
    """Generate payload for getting all clients from Replicon"""
    return {
        "page": "1",
        "pagesize": "999999",
        "columnUris": [
            "urn:replicon:client-list-column:client",
        ],
        "sort": [],
        "filterExpression": null
    }


def get_costpoint_plcs_payload():
    """Generate payload for getting PLCs from Costpoint"""
    return {
        "filter": {
            "id": "replicon_exp_plcs",
            "where": [
                {
                    "rsWhere": {
                        "rsId": "ADMUDT07_HDR",
                        "conditions": [
                        ],
                        "children": [
                        ]
                    }
                }
            ]
        }
    }


def get_project_udfs_payload():
    """Generate payload for getting project UDFs from Replicon"""
    return {
        "page": "1",
        "pagesize": "999999",
        "columnUris": [
            "urn:replicon:project-custom-field-list-column:project-custom-field",
        ],
        "sort": [],
        "filterExpression": null
    }


def process_clients_payload(item):
    """Generate payload for creating a client"""
    return {
        "client": {
            "target": {
                "uri": null,
                "name": item,
                "code": null,
                "parameterCorrelationId": null
            },
            "name": item,
            "code": null,
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


def get_project_team_members(dag_run):
    """Get project team members based on workforce data with complete structure"""
    assigned_users = []
    workforce_data = rail.result('get_workforce_user_costpoint')

    # Get all unique users from workforce data for this project
    seen_users = set()
    for workforce_item in workforce_data:
        for child in workforce_item['row'].get('children', []):
            emp_id = child['row']['data'].get('EMPL_ID')
            if emp_id and emp_id not in seen_users:
                seen_users.add(emp_id)
                user_detail = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_users_from_replicon'),
                    'employeeId',
                    emp_id,
                    'userDetails'
                )
                if user_detail:
                    assigned_users.append({
                        "resource": _build_assigned_resource_dict(user_detail['uri']),
                        "resourcePlaceholder": null,
                        "timeAndMaterials": null
                    })

    return assigned_users