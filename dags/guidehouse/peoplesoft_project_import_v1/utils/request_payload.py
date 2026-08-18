import uuid
import rail
from airflow.operators.python import get_current_context

INPUT_DATE_FORMAT = '%Y-%m-%d'

null = None

def does_wbs_exist():
    # Check only if the CURRENT project exists, not the parent project
    project_details = rail.result('get_project_details')
    return bool(project_details.get('current_project'))

def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else None

def should_assign_cp_project_custom_field(project_data):
    """
    Determine if CP_PROJECT should be assigned to custom field based on validations
    Returns True only if CP_PROJECT format is valid AND parent project exists
    """
    cp_project = project_data.get('cp_project', '').strip()
    if not cp_project:
        return False

    # Check format validation (from validate_optional_fields)
    optional_field_issues = rail.result("validate_optional_fields")
    cp_project_format_valid = not any("parent project assignment skipped" in issue for issue in optional_field_issues)

    # Check existence validation (from get_project_details - always available)
    project_details = rail.result('get_project_details', {})
    parent_exists = bool(project_details.get('parent_project'))

    return cp_project_format_valid and parent_exists

def should_update_client_assignment(customer_name):
    """
    Determine whether the incoming CUSTOMER_NAME differs from the client already
    assigned to the project in Replicon.

    Returning False keeps the client keys out of the update payload, so an unchanged
    client is not re-applied on every run - re-applying it records a client change in
    the project audit trail even when nothing actually changed.
    """
    incoming_client_name = (customer_name or '').strip()
    if not incoming_client_name:
        return False

    current_project = rail.result('get_project_details', {}).get('current_project')
    if not current_project:
        # New project - the client always has to be applied
        return True

    existing_client_names = [
        ((entry.get('client') or {}).get('name') or '').strip()
        for entry in (current_project.get('clients') or [])
    ]
    existing_client_names = [name for name in existing_client_names if name]

    # Skip only when this exact client is the sole client already assigned
    return not (len(existing_client_names) == 1 and
                existing_client_names[0].casefold() == incoming_client_name.casefold())

def get_log_message():
    """Generate detailed log message for project operations"""
    project_data = rail.result("load_project_data_from_query")
    project_number = project_data.get('project_id','')
    project_manager = project_data.get('project_manager', '')
    project_type = project_data.get('project_type', '')
    activity_type = project_data.get('activity_type', '')
    co_managers = project_data.get('co_manager', '')

    issues = []

    # Add optional field validation issues
    optional_field_issues = rail.result("validate_optional_fields")
    if optional_field_issues:
        issues.extend(optional_field_issues)

    if get_task_state("assign_project_manager_to_project") and get_task_state("assign_project_manager_to_project").lower() != "success":
        if project_manager:
            pm_validation = rail.result("get_project_manager_in_replicon", {})
            if pm_validation.get('is_enabled') == False and pm_validation.get('user_details'):
                pm_display = pm_validation['user_details'].get('displayText', project_manager)
                issues.append(f"project manager assignment skipped (employee ID '{project_manager}' ({pm_display}) is disabled in Replicon)")
            elif not pm_validation.get('user_details'):
                issues.append(f"project manager assignment skipped (employee ID '{project_manager}' not found in Replicon)")
            else:
                issues.append(f"project manager assignment skipped (employee ID '{project_manager}' could not be assigned)")
        else:
            issues.append("project manager assignment skipped (empty employee ID in Input file)")

    # Check for combined co-managers (CSV + parent) to match assignment logic
    combined_result = rail.result("combine_parent_and_csv_co_managers", {})
    has_csv_co_managers = bool(co_managers.strip())
    has_parent_co_managers = combined_result.get('combined_total', 0) > len(co_managers.split(';')) if co_managers else combined_result.get('combined_total', 0) > 0

    if has_csv_co_managers or has_parent_co_managers:
        assign_state = get_task_state("assign_co_managers_to_project")

        # Build comprehensive co-manager description for logging
        co_manager_description_parts = []
        if has_csv_co_managers:
            co_manager_description_parts.append(f"project co-managers '{co_managers}'")
        if has_parent_co_managers:
            parent_count = len(combined_result.get('parent_manager_uris', []))
            co_manager_description_parts.append(f"{parent_count} parent project manager(s)")

        co_manager_description = " and ".join(co_manager_description_parts)

        # Check if we have specific CSV co-manager issues to report
        csv_specific_issue_added = False

        if has_csv_co_managers:
            cm_response = rail.result("get_co_managers_in_replicon", {})
            disabled_users = cm_response.get('disabled_users', [])
            enabled_count = cm_response.get('enabled_count', 0)
            total_requested = cm_response.get('total_requested', 0)

            if disabled_users and enabled_count > 0:
                disabled_names = [f"{u['display_name']} ({u['employee_id']})" for u in disabled_users]
                issues.append(f"co-manager assignment partially failed for project co-managers (some users disabled in Replicon: {', '.join(disabled_names)})")
                csv_specific_issue_added = True

            elif total_requested == 0:
                # Users not found in Replicon - highest priority
                issues.append(f"co-manager assignment skipped for project co-managers (employee IDs '{co_managers}' not found in Replicon)")
                csv_specific_issue_added = True
            elif disabled_users and enabled_count == 0:
                # All users disabled in Replicon
                disabled_names = [f"{u['display_name']} ({u['employee_id']})" for u in disabled_users]
                issues.append(f"co-manager assignment skipped for project co-managers (employee IDs '{co_managers}' - all users disabled in Replicon: {', '.join(disabled_names)})")
                csv_specific_issue_added = True

        # Only add general assignment status if no CSV-specific issue was already added
        # Note: a "skipped" assign task means all co-managers are already assigned to the
        # project - that is a normal outcome and is intentionally not logged as an exception
        if not csv_specific_issue_added:
            if assign_state and assign_state.lower() not in ("success", "skipped"):
                # Assignment was attempted but failed for other reasons
                issues.append(f"co-manager assignment skipped ({co_manager_description} could not be assigned)")

    # Add IWO linking status
    cp_project = project_data.get('cp_project', '').strip()
    if cp_project:
        validation = rail.result("validate_parent_project", {})
        if validation and not validation.get('should_continue'):
            issues.append(f"IWO linking skipped ({validation.get('error', 'Unknown error')})")
        elif validation:
            link_created = get_task_state("create_iwo_project_link")
            if link_created and link_created.lower() == "success":
                parent_name = validation.get('parent_name', cp_project)
                # Use conditional text based on whether project is new or existing
                iwo_action = "updated" if does_wbs_exist() else "created"
                issues.append(f"IWO linking {iwo_action} to parent project '{parent_name}'")
            # Note: a skipped should_create_project_link means the link already exists -
            # that is a normal outcome and is intentionally not logged as an exception

    if project_type:
        if not rail.result('get_project_type_dropdown'):
            issues.append(f"project type assignment skipped ('{project_type}' not found in Replicon dropdown)")

    if activity_type:
        if not rail.result('get_task_type_dropdown'):
            issues.append(f"task type assignment skipped ('{activity_type}' not found in Replicon dropdown)")
    base_action = "updated" if does_wbs_exist() else "created"
    if issues:
        # Separate completed actions from skipped items (mutually exclusive)
        skipped_items = [issue for issue in issues if "skipped" in issue.lower()]
        completed_actions = [issue for issue in issues if issue not in skipped_items and ("created" in issue.lower() or "assigned" in issue.lower() or "partially failed" in issue.lower())]

        # Build message with appropriate connectors
        message_parts = [f"Project {project_number} {base_action} successfully"]
        if completed_actions:
            message_parts.append("and " + ", ".join(completed_actions))
        if skipped_items:
            message_parts.append("but " + ", ".join(skipped_items))

        issue_msg = " ".join(message_parts)

        # Success: Main operation successful + any additional operations are also successes
        # Exception: Any failed, skipped, or problematic operations
        main_operation_successful = "successfully" in issue_msg.lower()

        # Check if additional issues contain actual problems (not successes)
        has_actual_problems = any(
            "skipped" in issue.lower() or
            "failed" in issue.lower() or
            "could not" in issue.lower() or
            "partially failed" in issue.lower()
            for issue in issues
        )

        status = "Success" if main_operation_successful and not has_actual_problems else "Exception"

        return {
            "message": issue_msg,
            "status": status
        }
    else:
        return {
            "message": f"Project {project_number} {base_action} successfully",
            "status": "Success"
        }

def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['current_project']['uri']
        }
    return None

def create_project_rest_api(dag_run):

    project_data = rail.result("load_project_data_from_query")
    modifications = {
        "nameToApply": {
            "value": project_data.get('project_descr', '')
        },
        "codeToApply": {
            "value": project_data.get('project_id', '')
        },
        "statusToApply": {
            "name": map_project_status(project_data.get('project_status', 'A'))
        },
        "isTimeEntryAllowed": "0",
        "isProjectLeaderApprovalRequired": "0",
        "keyValuesToApply": [
            {
                "keyUri": "urn:replicon:project-key-value-key:project-management-type",
                "value": {
                    "uri": "urn:replicon:project-management-type:managed",
                    "slug": None,
                    "bool": None,
                    "date": None,
                    "number": None,
                    "text": None,
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "collection": []
                }
            }
        ],
        "customFieldsToApply": [
            {
                "customField": {
                "uri": dag_run.conf["sourcesystem_custom_field_uri"],
                "name": null,
                "groupUri": null
                },
                "text": null,
                "date": null,
                "dropDownOption": {
                "uri": rail.result('get_source_system_dropdown'),
                "name": null
                },
                "number": null
            },
            {
                "customField": {
                "uri": dag_run.conf["enforce_custom_field_uri"],
                "name": null,
                "groupUri": null
                },
                "text": null,
                "date": null,
                "dropDownOption": {
                "uri": rail.result('get_enforce_dropdown'),
                "name": null
                },
                "number": null
            }
        ]
    }

    if project_data.get('project_start_date'):
        modifications["startDateToApply"] = {
            "date": rail.parse_date(project_data['project_start_date'], INPUT_DATE_FORMAT)
        }

    if project_data.get('project_end_date'):
        modifications["endDateToApply"] = {
            "date": rail.parse_date(project_data['project_end_date'], INPUT_DATE_FORMAT)
        }

    # Assign client if a customer name is present (comma-separated values are treated as-is)
    customer_name = project_data.get('customer_name', '')

    if customer_name:
        modifications["clientBillingAllocationMethodToApply"] = "urn:replicon:client-billing-allocation-method:split"
        modifications["clientAssignmentsSchedulesToApply"] = {
            "clients": [{
            "client": {
                "uri": None,
                "name": customer_name,
                "code": None,
                "parameterCorrelationId": None
            },
            "costAllocationPercentage": "100"
            }],
            "effectiveDate": None
        }
        modifications["timeAndMaterials"] = {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
            "billingRateFrequency": None,
            "billingRateFrequencyDuration": None,
            "billingRates": []
        }

    if project_data.get('project_type') and rail.result('get_project_type_dropdown'):
        modifications["customFieldsToApply"].append({
            "customField": {
                "uri": dag_run.conf["projecttype_custom_field_uri"],
                "name": null,
                "groupUri": null
            },
            "text": null,
            "date": null,
            "dropDownOption": {
                "uri": rail.result('get_project_type_dropdown'),
                "name": null
            },
            "number": null
        })

    # Add Cost Center assignment if DEPT_NAME is provided
    # Note: Cost center creation logic already handles DEPT_CODE validation for descriptions
    # If cost center was created successfully (with or without description), assign division by name
    if project_data.get('dept_name'):
        modifications["divisionToApply"] = {
            "division": {
                "uri": None,
                "parentUri": None,
                "name": project_data.get('dept_name')  # Pass name to find and assign existing cost center
            }
        }

    # Always assign PeopleSoft service center for Financial System Group
    peoplesoft_uri = dag_run.conf.get('peoplesoft_service_center_uri')
    if peoplesoft_uri:
        modifications["serviceCenterToApply"] = {
            "serviceCenter": {
                "uri": peoplesoft_uri,
                "parentUri": None,
                "name": None
            }
        }

    # Add CP_PROJECT custom field only if value passes all validations
    if (project_data.get('cp_project') and
        dag_run.conf.get("owning_parent_project_custom_field_uri") and
        should_assign_cp_project_custom_field(project_data)):
        modifications["customFieldsToApply"].append({
            "customField": {
                "uri": dag_run.conf["owning_parent_project_custom_field_uri"],
                "name": None,
                "groupUri": None
            },
            "text": project_data.get('cp_project', '').strip(),
            "date": None,
            "dropDownOption": None,
            "number": None
        })

    return {
        "target": None,
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def create_project_with_tasks_payload(dag_run):
    project_data = rail.result("load_project_data_from_query")
    modifications = {
        "nameToApply": {
            "value": project_data.get('project_descr', '')
        },
        "statusToApply": {
            "name": map_project_status(project_data.get('project_status', 'A'))
        },
        "isTimeEntryAllowed": "0",
    }

    if not does_wbs_exist():
        modifications["codeToApply"] = {
            "value": project_data.get('project_id', '')
        }

    if project_data.get('project_start_date'):
        modifications["startDateToApply"] = {
            "date": rail.parse_date(project_data['project_start_date'], INPUT_DATE_FORMAT)
        }

    if project_data.get('project_end_date'):
        modifications["endDateToApply"] = {
            "date": rail.parse_date(project_data['project_end_date'], INPUT_DATE_FORMAT)
        }

    modifications["customFieldsToApply"] = [
        {
            "customField": {
                "uri": dag_run.conf["sourcesystem_custom_field_uri"],
                "name": null,
                "groupUri": null
            },
            "text": null,
            "date": null,
            "dropDownOption": {
                "uri": rail.result('get_source_system_dropdown'),
                "name": null
            },
            "number": null
        },
        {
            "customField": {
                "uri": dag_run.conf["enforce_custom_field_uri"],
                "name": null,
                "groupUri": null
            },
            "text": null,
            "date": null,
            "dropDownOption": {
                "uri": rail.result('get_enforce_dropdown'),
                "name": null
            },
            "number": null
        }
    ]

    if project_data.get('project_type') and rail.result('get_project_type_dropdown'):
        modifications["customFieldsToApply"].append({
            "customField": {
                "uri": dag_run.conf["projecttype_custom_field_uri"],
                "name": null,
                "groupUri": null
            },
            "text": null,
            "date": null,
            "dropDownOption": {
                "uri": rail.result('get_project_type_dropdown'),
                "name": null
            },
            "number": null
        })

    # Add Cost Center assignment if DEPT_NAME is provided
    # Note: Cost center creation logic already handles DEPT_CODE validation for descriptions
    # If cost center was created successfully (with or without description), assign division by name
    if project_data.get('dept_name'):
        modifications["divisionToApply"] = {
            "division": {
                "uri": None,
                "parentUri": None,
                "name": project_data.get('dept_name')  # Pass name to find and assign existing cost center
            }
        }

    # Assign client if a customer name is present (comma-separated values are treated as-is).
    # Only applied when it differs from the client already on the project, so an unchanged
    # client is not re-applied on every run and does not pollute the project audit trail.
    customer_name = project_data.get('customer_name', '')
    if should_update_client_assignment(customer_name):
        modifications["clientBillingAllocationMethodToApply"] = "urn:replicon:client-billing-allocation-method:split"
        modifications["clientAssignmentsSchedulesToApply"] = {
            "clients": [{
                "client": {
                    "uri": None,
                    "name": customer_name,
                    "code": None,
                    "parameterCorrelationId": None
                },
                "costAllocationPercentage": "100"
            }],
            "effectiveDate": None
        }

    # Always assign PeopleSoft service center for Financial System Group
    peoplesoft_uri = dag_run.conf.get('peoplesoft_service_center_uri')
    if peoplesoft_uri:
        modifications["serviceCenterToApply"] = {
            "serviceCenter": {
                "uri": peoplesoft_uri,
                "parentUri": None,
                "name": None
            }
        }

    # Add CP_PROJECT custom field only if value passes all validations
    if (project_data.get('cp_project') and
        dag_run.conf.get("owning_parent_project_custom_field_uri") and
        should_assign_cp_project_custom_field(project_data)):
        modifications["customFieldsToApply"].append({
            "customField": {
                "uri": dag_run.conf["owning_parent_project_custom_field_uri"],
                "name": None,
                "groupUri": None
            },
            "text": project_data.get('cp_project', '').strip(),
            "date": None,
            "dropDownOption": None,
            "number": None
        })

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def map_project_status(status):
    if status and status.upper() in ['A']:
         return 'In Progress'
    elif status and status.upper() in ['I']:
        return 'Completed'
    else:
         return 'In Progress'

def get_task_payload(dag_run, action, data):
    return list(map(lambda task: {
        "target": None if action == "add" else {"uri": task['existing_uri']},
        "taskModificationToApply": {
                "name": task['activity'],
                "codeToApply": {
                    "value": task['activity_descr']
                },
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "timeEntryStartDateToApply": {
                    "date": rail.parse_date(task['activity_start_date'], INPUT_DATE_FORMAT)
                },
                "timeEntryEndDateToApply": {
                    "date": rail.parse_date(task['activity_end_date'], INPUT_DATE_FORMAT)
                },
                "isTimeEntryAllowed": "1",
                "isClosed": "1" if task['activity_status'].upper() == 'I' else "0",
                "customFieldsToApply": [
                    {
                        "customField": {
                            "uri": dag_run.conf.get('task_custom_field_uri'),
                            "name": null,
                            "groupUri": null
                        },
                        "text": null,
                        "date": null,
                        "dropDownOption": {
                            "uri": rail.result('get_task_type_dropdown'),
                            "name": null
                        },
                        "number": null
                    }
                ] if dag_run.conf.get('task_custom_field_uri') and rail.result('get_task_type_dropdown') else []
            }
    }, data))

def get_project_uri():
    if rail.result('create_project_in_replicon'):
        return rail.result('create_project_in_replicon')
    else:
        return rail.result('update_project')['uri']

def build_create_cost_center_payload(dag_run):
    modifications = {
        "name": dag_run.conf.get('cost_center_name'),
        "codeToApply": null,
        "isEnabled": "true"
    }
    cost_center_code = dag_run.conf.get('cost_center_code')
    if cost_center_code and cost_center_code.strip():
        modifications["descriptionToApply"] = {"value": cost_center_code}

    return {
                "division": null,
                "modifications": modifications,
                "unitOfWorkId": str(uuid.uuid4())
            }

def remove_all_users_timesheet_access():
    """
    Remove "All Users" department from project timesheet access
    Uses GraphQL mutation: RemoveProjectTimesheetAccessMember

    The "All Users" department always has ID 1 in Replicon tenants

    Returns:
        dict: GraphQL mutation with variables for removing All Users access
    """
    project_uri = rail.result("create_project_in_replicon")
    all_users_department_uri = "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"

    return {
        "operationName": "RemoveProjectTimesheetAccessMember",
        "variables": {
            "projectId": project_uri,
            "accessMemberId": all_users_department_uri,
            "isResource": False
        },
        "query": """mutation RemoveProjectTimesheetAccessMember($projectId: String!, $accessMemberId: String!, $isResource: Boolean) {
                removeProjectTimesheetAccessMember2(
                    projectId: $projectId
                    accessMemberId: $accessMemberId
                    isResource: $isResource
                )
            }
        """
    }

def assign_pm_permissions_payload(dag_run):
    return {
        "user": {
            "uri": rail.result("get_project_manager_in_replicon")["user_details"]["uri"]
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": rail.result("determine_missing_permissions"),
                "policyUrisToRemovePermissionSet": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def assign_co_manager_permissions_payload(item, dag_run):
    """
    Build payload for assigning permissions to individual co-manager.

    Args:
        item: Dict containing 'userUri' and 'permissionSetUri' for the co-manager
        dag_run: Airflow DAG run context

    Returns:
        Dict containing the API payload for ApplyUserModifications3
    """
    return {
        "user": {
            "uri": item["userUri"]
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": [item["permissionSetUri"]],
                "policyUrisToRemovePermissionSet": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_batched_add_task_payload(item,dag_run):
    return {
        "project": {
            "uri": get_project_uri(),
        },
        "taskHierarchy": get_task_payload(dag_run, "add", item),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_batched_update_task_payload(item,dag_run):
    return {
        "project": {
            "uri": get_project_uri(),
        },
        "taskHierarchy": get_task_payload(dag_run, "update", item),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_create_client_payload_peoplesoft(dag_run):
    return {
        "client": {
            "target": {
                "name": dag_run.conf['client_name']
            },
            "name": dag_run.conf['client_name'],
            "code": dag_run.conf['client_id'],
            "isActive": True
        }
    }

def get_co_manager_sharing_payload():
    """Enhanced to include parent project managers from IWO linking"""
    # Get existing sharing assignments from the GET call
    existing_assignments = rail.result("get_existing_sharing_assignments", [])
    existing_shared_uris = []

    for assignment in existing_assignments:
        if assignment.get('user', {}).get('uri'):
            existing_shared_uris.append(assignment['user']['uri'])

    # Get enabled co-manager URIs from the CSV response structure
    co_manager_response = rail.result("get_co_managers_in_replicon", {})
    csv_co_manager_uris = co_manager_response.get('enabled_user_uris', [])

    # Get parent project manager URIs from IWO linking
    parent_manager_uris = []
    if rail.result("combine_parent_and_csv_co_managers"):
        parent_manager_uris = rail.result("combine_parent_and_csv_co_managers").get('parent_manager_uris', [])

    # Combine all URIs, remove duplicates
    all_shared_uris = list(set(existing_shared_uris + csv_co_manager_uris + parent_manager_uris))

    return {
        "projectUri": get_project_uri(),
        "sharedUris": all_shared_uris
    }

def get_division_payload():
    """Generate payload for DivisionListService GetData

    Note: DEPT_CODE from CSV corresponds to division description field in Replicon
    """
    return {
        "page": 1,
        "pagesize": 1000,
        "columnUris": [
            "urn:replicon:division-list-column:effectively-enabled",
            "urn:replicon:division-list-column:name",
            "urn:replicon:division-list-column:description"
        ],
        "sort": [],
        "filterExpression": null
    }
