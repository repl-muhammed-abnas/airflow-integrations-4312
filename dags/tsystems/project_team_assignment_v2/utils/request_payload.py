import rail
from datetime import datetime
from uuid import uuid4
import json

DATE_FORMAT = '%d.%m.%Y'

def parse_date(date_str):
    dt = datetime.strptime(date_str, DATE_FORMAT)
    return {"year": dt.year, "month": dt.month, "day": dt.day}

def update_user_assignment_date_range_payload():
    if not rail.result('get_user_assigned_to_project'):
        daterange = rail.result('extract_capacity_date_range')
    else:
        daterange = rail.result('compare_assignment_date_range')
    return {
        "projectUri": rail.result('get_project_from_costobject_id')['project_uri'],
        "resourceUri": rail.result('get_user_from_individual_id')[0]['user_uri'],
        "dateRange": {
            "startDate": parse_date(daterange['start_date'].strftime(DATE_FORMAT)),
            "endDate": parse_date(daterange['end_date'].strftime(DATE_FORMAT))
        }
    }

def assign_user_to_project_payload():
    return {
        "projectUri": rail.result('get_project_from_costobject_id')['project_uri'],
        "resourceUri": [rail.result('get_user_from_individual_id')[0]['user_uri']],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }

def per_day_allocation_payload(dag_run):
    # datetime.fromisoformat(dag_run.conf['allocation_date'].replace('Z', '+00:00')).strftime(DATE_FORMAT),
    date = parse_date(datetime.fromisoformat(dag_run.conf['allocation_date'].replace('Z', '+00:00')).strftime(DATE_FORMAT),)

    # Convert decimal capacity to hours, minutes, and seconds
    decimal_hours = float(dag_run.conf['capacity_amount'])
    total_seconds = int(decimal_hours * 3600)

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return {
        "projectUri": dag_run.conf['project_uri'],
        "resourceUri": dag_run.conf['user_uri'],
        "dateRange": {"startDate": date, "endDate": date},
        "allocationTime": {
            "hoursPerDay": {
                "hours": str(hours),
                "minutes": str(minutes), "seconds": str(seconds), "milliseconds": "0", "microseconds": "0"
            }
        },
        "resourceAllocationOptionUris": [
            "urn:replicon:resource-allocation-option:force-allocation-on-days-off",
            "urn:replicon:resource-allocation-option:force-allocation-on-holidays",
            "urn:replicon:resource-allocation-option:force-allocation-on-time-off"
        ]
    }

def remove_employee_type_restriction_payload():
    return {
        "projectUri": rail.result('get_project_from_costobject_id')['project_uri'],
        "teamMemberDataAccessScopes": []
    }

def add_employee_type_restriction_payload():
    project_access_scopes = rail.result('get_assign_team_from_for_the_project')
    scope_keys = ['locations', 'divisions', 'costCenters', 'serviceCenters', 'departmentGroups']
    
    # Get existing employeetypegroups (already in name format)
    employee_type_groups = project_access_scopes.get('employeeTypeGroups', [])
    
    # Add the specified employee types with name format
    specified_employee_types = [
        {"name": "External Contractors"}, {"name": "External Freelancer"},
        {"name": "External Manual"}, {"name": "External services"}
    ]
    
    # Combine existing employee type groups with specified ones
    all_employee_type_groups = employee_type_groups + specified_employee_types
    
    return {
        "projectUri": rail.result('get_project_from_costobject_id')['project_uri'],
        "teamMemberDataAccessScopes": [{
            **{scope: project_access_scopes.get(scope, []) for scope in scope_keys},
            "employeeTypeGroups": all_employee_type_groups
        }]
    }

def unassign_project_rate_payload():
    return {
        "projectTeamMemberBillingRate": {
            "projectUri": rail.result('get_project_from_costobject_id')['project_uri'],
            "resourceUri": rail.result('get_user_from_individual_id')[0]['user_uri'],
            "billingRateUris": rail.result('get_user_assigned_to_project')['billing_rate_uris'],
            "billingRateCopyOptionUri": "urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client",
            "defaultBillingRateUri":rail.result('get_user_assigned_to_project')['default_billing_rate_uri']
        }
    }

def get_team_assignment_blob_data(dag_run):
    return {
        "assignment_id": dag_run.conf['assignment_id'],
        "cost_object_id": dag_run.conf['cost_object_id'],
        "individual_id": dag_run.conf['individual_id'],
        "assignment_search_period_end": dag_run.conf['search_period_end'],
        "assignment_search_period_start": dag_run.conf['search_period_start']
    }

def add_project_data_to_blob_param(dag_run, key_namespace):
    return {
        "keyNamespace": key_namespace,
        "keyValue": {
            "key": get_team_assignment_blob_data(dag_run)["assignment_id"],
            "jsonValue": json.dumps([get_team_assignment_blob_data(dag_run)])
        }
    }

def get_all_task():
    return {
        "pageIndex": "1",
        "pageSize": "100000",
        "projectUris": [
            rail.result('get_project_from_costobject_id')['project_uri']
        ]
    }

def add_users_to_all_task():
    return {
        "projectUri": rail.result('get_project_from_costobject_id')['project_uri'],
        "resourceUri": rail.result('get_user_from_individual_id')[0]['user_uri'],
        "taskUris": rail.result('get_all_tasks_for_project')
    }
