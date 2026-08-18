"""
Unisys Project Import - Request Payload Utilities
Implements Unisys-specific business logic including field reversal for tasks
"""
import uuid
from datetime import datetime
import rail
from airflow.operators.python import get_current_context

INPUT_DATE_FORMAT = '%d-%b-%Y'

def does_wbs_exist():
    return bool(rail.result('get_project_details'))

def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else None

def get_log_message():
    """Generate detailed log message for project operations"""
    project_data = rail.result("load_project_data_from_query")
    project_number = project_data.get('projectnumber', 'Unknown')
    project_manager = project_data.get('projectmanager', '')
    msg =''

    if get_task_state("assign_project_manager_to_project").lower() != "success":
        if project_manager:
            msg = f" but project manager assignment is skipped due to employee ID '{project_manager}' not found in Replicon system"
        else:
            msg = " but project manager assignment is skipped due to empty employee ID in CSV file"

    if does_wbs_exist():
        return {
            "message": f"Project {project_number} updated successfully" + msg,
            "status": "Exception" if msg else "Success"
        }
    else:
        return {
            "message": f"Project {project_number} created successfully" + msg,
            "status": "Exception" if msg else "Success"
        }

def get_create_project_target_param():
    """Get target parameter for project creation/update"""
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def get_project_status_payload(dag_run):
    return {
        "target": {
            "uri": rail.result("create_project_in_replicon")
        },
        "modifications": {
            "statusToApply": {
                "name": "In Progress" if rail.result(
                        "load_project_data_from_query")['projectstatus'].strip().upper() == "ACTIVE" else "Completed"
            },
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['project_custom_field_uri']
                    },
                    "dropDownOption": {
                        "name": "Client"
                    }
                }
            ]
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

# ========== PROJECT CREATION (GRAPHQL) ==========
def create_project_graphql(dag_run):
    """
    Create NEW project using Polaris GraphQL mutation
    Uses all required Unisys fields from CSV

    CSV Fields: projectnumber, projectname, projectstartdate, projectenddate, projectstatus, companycode
    """
    project_data = rail.result("load_project_data_from_query")

    # Get division URI from companycode
    division_uri = rail.find_first_by_attr_and_get_attr(
        rail.load_all_records(dag_run.conf['get_division_uris']),
        'displayText',
        project_data.get('companycode', ''),
        'uri',
        None
    )

    # Format dates
    start_date = None
    end_date = None

    if project_data.get('projectstartdate'):
        start_date = datetime.strptime(project_data['projectstartdate'], INPUT_DATE_FORMAT).strftime('%Y-%m-%d')

    if project_data.get('projectenddate'):
        end_date = datetime.strptime(project_data['projectenddate'], INPUT_DATE_FORMAT).strftime('%Y-%m-%d')

    # Build mutation dynamically based on available fields
    variables = {
        "name": project_data.get('projectname', ''),
        "code": project_data.get('projectnumber', ''),
        "startDate": start_date,
        "endDate": end_date
    }

    # Build the mutation fields
    mutation_fields = """
                name: $name
                code: $code
                startDate: $startDate
                endDate: $endDate
                projectManagementType: "urn:replicon:project-management-type:managed"
                isTimeEntryAllowed: false
                isProjectLeaderApprovalRequired: false
                projectManagerReference: null"""

    query_params = """
            $name: String!,
            $code: String!,
            $startDate: Date,
            $endDate: Date"""

    # Add division if available (GroupInput object)
    if division_uri:
        variables["divisionUri"] = division_uri
        query_params += ",\n            $divisionUri: String"
        mutation_fields += "\n                division: { id: $divisionUri }"

    return {
        "operationName": "CreateProject",
        "variables": variables,
        "query": f"""mutation CreateProject({query_params}
        ) {{
            addProject2(projectInput: {{{mutation_fields}
            }}) {{
                project {{
                    id
                    uri
                }}
                errors {{
                    displayText
                    failureUri
                }}
            }}
        }}"""
    }


# ========== PROJECT UPDATE (REST API) ==========
def create_project_with_tasks_payload(dag_run):
    project_data = rail.result("load_project_data_from_query")

    # Get division URI from companycode
    division_uri = rail.find_first_by_attr_and_get_attr(
        rail.load_all_records(dag_run.conf['get_division_uris']),
        'displayText',
        project_data.get('companycode', ''),
        'uri',
        ''
    )

    # Project payload with all required fields
    modifications = {
        "nameToApply": {
            "value": project_data.get('projectname', '')
        },
        "statusToApply": {
            "name": map_project_status(project_data.get('projectstatus', 'Active'))
        },
        "isTimeEntryAllowed": "0",  # Project level: time entry NOT allowed
    }

    # Add code only for new projects
    if not does_wbs_exist():
        modifications["codeToApply"] = {
            "value": project_data.get('projectnumber', '')
        }

    # Add division if available
    if division_uri:
        modifications["divisionToApply"] = {
            "division": {
                "uri": division_uri,
            }
        }

    # Add start date if provided
    if project_data.get('projectstartdate'):
        modifications["startDateToApply"] = {
            "date": rail.parse_date(project_data['projectstartdate'], INPUT_DATE_FORMAT)
        }

    # Add end date if provided
    if project_data.get('projectenddate'):
        modifications["endDateToApply"] = {
            "date": rail.parse_date(project_data['projectenddate'], INPUT_DATE_FORMAT)
        }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

# Helper functions
def format_date(date_string):
    """Convert DD-MMM-YYYY to proper date format"""
    if not date_string or date_string.strip() == '':
        return None

    # Handle DD-MMM-YYYY format (case insensitive)
    try:
        from datetime import datetime
        date_obj = datetime.strptime(date_string.upper(), '%d-%b-%Y')
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None


def map_project_status(status):
    """Map project status - Unisys only allows Active or Closed"""
    if status and status.lower() in ['active']:
        return 'In Progress'
    elif status and status.lower() in ['closed']:
        return 'Completed'
    else:
        return 'In Progress'  # Default to Active for invalid values


def get_error_log_properties():
    """Log properties for general errors"""
    try:
        project_data = rail.result("load_project_data_from_query")
        return {
            "projectnumber": project_data.get('projectnumber', ''),
            "projectname": project_data.get('projectname', ''),
            "taskcode": '',
            "taskname": '',
            "action": "Error",
            "status": "Error",
            "details": rail.get_error_message()
        }
    except:
        return {
            "projectnumber": '',
            "projectname": '',
            "taskcode": '',
            "taskname": '',
            "action": "Error",
            "status": "Error",
            "details": rail.get_error_message()
        }

def get_task_payload(dag_run, action, data):
    """Generate task payload with field reversal for add/update operations"""
    return list(map(lambda task: {
        "target": None if action == "add" else {"uri": task['existing_uri']},
        "taskModificationToApply": {
                "name": task['taskcode'],  # Field reversal: taskcode → name
                "codeToApply": {
                    "value": task['taskname']  # Field reversal: taskname → code
                },
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "timeEntryStartDateToApply": {
                    "date": rail.parse_date(task['taskstartdate'], INPUT_DATE_FORMAT)
                },
                "timeEntryEndDateToApply": {
                    "date": rail.parse_date(task['taskenddate'], INPUT_DATE_FORMAT)
                },
                "isTimeEntryAllowed": "1",
                "customFieldsToApply": [
                    {
                        "customField": {
                            "uri": dag_run.conf.get('task_custom_field_uri')
                        },
                        "text": task['taskpaycode'] if task.get('taskpaycode') else "105"
                    }
                ] if dag_run.conf.get('task_custom_field_uri') else []
            }
    }, data))

def get_project_uri():
    """Get project URI from either create (GraphQL) or update (REST) operation"""
    if rail.result('create_project_in_replicon'):
        return rail.result('create_project_in_replicon')
    else:
        return rail.result('update_project')['uri']

def get_batch_put_task_payload(dag_run):
    return {
        "project": {
            "uri": get_project_uri(),
        },
        "taskHierarchy": get_task_payload(dag_run,"add",rail.result("get_all_task_to_add_update")['add']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_update_task_payload(dag_run):
    return {
        "project": {
            "uri": get_project_uri(),
        },
        "taskHierarchy": get_task_payload(dag_run,"update",rail.result("get_all_task_to_add_update")['update']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
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
            "uri": rail.result("get_project_manager_in_replicon")["uri"]
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": rail.result("determine_missing_permissions"),
                "policyUrisToRemovePermissionSet": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


# ========== BATCH PROCESSING PAYLOADS ==========

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
