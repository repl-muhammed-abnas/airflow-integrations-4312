from datetime import datetime
from uuid import uuid4
import rail
from dxctechnology.psa_resource_assignment_v2.utils import custom_methods

null = None


def active_user(load_report_data):
    """Extract active users from report data"""
    jsonValue = rail.load_all_records(rail.result(load_report_data))
    return list(
        map(lambda x: {
            'username': x['User Name'],
            'loginname': x['Login Name'],
            'employeeid': x['Employeeid'],
            'iapernerid': x['IA Perner ID'],
            'cwfalternateid': x['CWF C1 alternate ID'],
            'useruri': x['UserUri'],
            'userstatus': x['User Status'],
            'companycodefullpath': x['Company Code (Current) (Full Path)']
        }, jsonValue))


def group_users_by_wbs(valid_records_task, active_users_task):
    """
    Group users by WBS for bulk processing.
    Returns a list of dictionaries, each containing a WBS and all its users.
    """
    valid_records = rail.load_all_records(rail.result(valid_records_task))
    active_users = rail.result(active_users_task)

    # Create a lookup dictionary for active users
    user_lookup = {user['employeeid']: user for user in active_users}

    # Group records by WBS
    wbs_groups = {}
    for record in valid_records:
        wbs = record['WBS']
        if wbs not in wbs_groups:
            wbs_groups[wbs] = []

        # Find user details
        user_info = user_lookup.get(record['PERN'])

        if user_info:
            wbs_groups[wbs].append({
                'empid': record['PERN'],
                'assignmentStartDate': record['StartDate'],
                'assignmentEndDate': record['EndDate'],
                'useruri': user_info['useruri'],
                'employeeid': user_info['employeeid'],
                'companycode': user_info['companycodefullpath']
            })
        else:
            # User not found in active users
            wbs_groups[wbs].append({
                'empid': record['PERN'],
                'assignmentStartDate': record['StartDate'],
                'assignmentEndDate': record['EndDate'],
                'useruri': None,
                'employeeid': record['PERN'],
                'companycode': None
            })

    # Convert to list format for processing
    result = []
    for wbs, users in wbs_groups.items():
        result.append({
            'wbs': wbs,
            'users': users
        })

    return result


def validate_bulk_assignment_dates(dag_run):
    """
    Validate assignment dates for all users in bulk.
    Returns list of validated users with their date status.
    """
    users = dag_run.conf.get('users', [])
    validated_users = []

    for user in users:
        start_date = get_replicon_date(user['assignmentStartDate'])
        end_date = get_replicon_date(user['assignmentEndDate'])

        validated_users.append({
            'empid': user['empid'],
            'useruri': user.get('useruri'),
            'companycode': user.get('companycode'),
            'assignmentStartDate': user['assignmentStartDate'],
            'assignmentEndDate': user['assignmentEndDate'],
            'start_date_valid': start_date is not None,
            'end_date_valid': end_date is not None,
            'user_exists': user.get('useruri') is not None
        })

    return validated_users


def separate_users_by_validity():
    """
    Separate users into valid and invalid based on validation results.
    """
    validated_users = rail.result('validate_bulk_assignments')

    valid_users = []
    invalid_users = []

    for user in validated_users:
        if not user['start_date_valid'] or not user['end_date_valid']:
            invalid_users.append({
                'empid': user['empid'],
                'reason': 'Gsap PSA Resource Assignment Sync Skipped - Either Start Date or End Date is not in correct format'
            })
        elif not user['user_exists']:
            invalid_users.append({
                'empid': user['empid'],
                'reason': 'Gsap PSA Resource Assignment Sync - Employee is not present in Replicon'
            })
        else:
            valid_users.append({
                'empid': user['empid'],
                'useruri': user['useruri'],
                'companycode': user['companycode'],
                'assignmentStartDate': user['assignmentStartDate'],
                'assignmentEndDate': user['assignmentEndDate']
            })

    return {
        'valid_users': valid_users,
        'invalid_users': invalid_users
    }


def categorize_users_for_bulk_operation():
    """
    Categorize users based on existing assignments for bulk operations.
    Separates users into:
    - users_to_add: New users to be added to project (Step 1: BulkUpdateProjectTeamMembersAssignment)
    - users_to_update: Existing users (already assigned, skip Step 1)
    - users_for_date_range_update: ALL users (new + existing) for date range updates (Step 2)
    - users_needing_child_wbs: Users with division mismatch needing child WBS processing
    """
    valid_users = rail.result('separate_valid_invalid_users')['valid_users']
    existing_assignments = rail.result('get_all_project_team_assignments')
    project_details = rail.result('get_project_details_based_on_wbs')

    # Get project division
    project_division = project_details.get('division', {}).get('displayText', '')

    # Create lookup for existing assignments
    existing_user_uris = set([assignment['resource']['uri'] for assignment in existing_assignments])

    users_to_add = []
    users_to_update = []
    users_needing_child_wbs = []
    users_for_date_range_update = []

    for user in valid_users:
        # Check division match
        user_division = (user.get('companycode', '') or '').split('/')[-1].strip()
        division_matches = user_division == project_division

        if not division_matches:
            # Division mismatch - needs child WBS processing
            users_needing_child_wbs.append(user)
        elif user['useruri'] in existing_user_uris:
            # User already assigned - skip Step 1, but still needs date range update (Step 2)
            users_to_update.append(user)
            users_for_date_range_update.append(user)
        else:
            # New user - needs assignment (Step 1) and date range update (Step 2)
            users_to_add.append(user)
            users_for_date_range_update.append(user)

    return {
        'users_to_add': users_to_add,
        'users_to_update': users_to_update,
        'users_for_date_range_update': users_for_date_range_update,
        'users_needing_child_wbs': users_needing_child_wbs
    }


def project_division(task_detail):
    """Check if project division matches user division"""
    data = rail.result(task_detail)['division']['displayText']
    user_div = custom_methods.get_conf()['companycode'].split("/")[-1].strip()
    return user_div == data


def get_replicon_date(date_str):
    """Convert date string to Replicon date format"""
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def project_wbs_type():
    """Get WBS type from project details"""
    data = rail.result('get_child_project_details')[
        'extensionFieldValues']
    filter_wbs = list(
        filter(lambda x: x['definition']['displayText'] == 'WBS Type', data))
    return filter_wbs[0]['tag']['displayText'] if filter_wbs else ""


def validate_users_for_child_wbs():
    """
    Validate users for child WBS assignment.
    Checks division matching and other criteria.
    """
    users = rail.get_current_context()['dag_run'].conf.get('users', [])
    child_project = rail.result('get_child_project_details')

    # Get child WBS division
    child_division = child_project.get('division', {}).get('displayText', '')

    validated_users = []
    for user in users:
        user_division = (user.get('companycode', '') or '').split('/')[-1].strip()
        division_matches = user_division == child_division

        validated_users.append({
            'empid': user['empid'],
            'useruri': user.get('useruri'),
            'companycode': user.get('companycode'),
            'assignmentStartDate': user['assignmentStartDate'],
            'assignmentEndDate': user['assignmentEndDate'],
            'division_matches': division_matches,
            'child_division': child_division,
            'user_division': user_division
        })

    return validated_users


def separate_users_by_division_match():
    """
    Separate users based on division matching with child WBS.
    """
    validated_users = rail.result('validate_users_for_child_wbs')

    valid_division = []
    invalid_division = []

    for user in validated_users:
        if user['division_matches']:
            valid_division.append({
                'empid': user['empid'],
                'useruri': user['useruri'],
                'companycode': user['companycode'],
                'assignmentStartDate': user['assignmentStartDate'],
                'assignmentEndDate': user['assignmentEndDate']
            })
        else:
            invalid_division.append({
                'empid': user['empid'],
                'user_division': user['user_division'],
                'child_division': user['child_division']
            })

    return {
        'valid_division': valid_division,
        'invalid_division': invalid_division
    }


def categorize_users_for_child_wbs_bulk():
    """
    Categorize users for bulk operations on child WBS.
    Both users_to_add and users_to_update are processed via CreateProjectOrApplyModifications
    which handles assignment and date ranges in a single bulk call.
    """
    valid_users = rail.result('separate_valid_invalid_division')['valid_division']
    existing_assignments = rail.result('get_child_project_team_assignment')

    # Create lookup for existing assignments
    existing_user_uris = set([assignment['resource']['uri'] for assignment in existing_assignments])

    users_to_add = []
    users_to_update = []

    for user in valid_users:
        if user['useruri'] in existing_user_uris:
            # User already assigned - needs date range update via bulk call
            users_to_update.append(user)
        else:
            # New user - added via CreateProjectOrApplyModifications with date ranges
            users_to_add.append(user)

    return {
        'users_to_add': users_to_add,
        'users_to_update': users_to_update,
        'users_for_date_range_update': valid_users
    }


def do_format_logs():
    """
    CR 02053847: Format and consolidate logs to ensure only 1 log entry per input record.
    Uses custom_methods.get_filtered_logs for consolidation.
    """
    log_artifacts = []
    log_records = []

    master_log = rail.result("create_master_log")
    if master_log:
        if isinstance(master_log, list):
            log_artifacts.extend(master_log)
        else:
            log_artifacts.append(master_log)

    logs = rail.result("gather_wbs_process_logs")

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    # CR 02053847: Apply log filtering/consolidation to ensure 1 log per input record
    final_log_records = custom_methods.get_filtered_logs(log_records)

    rail.set_result(key="get_successful_records", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_errored_records", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_exception_records", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))
    rail.set_result(key="get_skipped_records", val=len(list(filter(lambda item: item['status']=="Skipped", final_log_records))))
    print(final_log_records)

    return final_log_records


# ============================================================================
# Batching Functions for PSA Resource Assignment V2
# ============================================================================

def deduplicate_users_by_uri(users, uri_key='useruri'):
    """
    Remove duplicate users based on user URI, keeping the last occurrence.
    This ensures each user appears only once in the API call.
    """
    seen = {}
    for user in users:
        uri = user.get(uri_key)
        if uri:
            seen[uri] = user  # Last occurrence wins
    return list(seen.values())


def batch_users_for_assignment(batch_size=50):
    """
    Batch users for BulkUpdateProjectTeamMembersAssignment API calls.
    Only includes users_to_add (new users not yet assigned to project).
    Deduplicates users by useruri to avoid processing same user multiple times.
    Returns list of batches, each containing up to batch_size user URIs.
    """
    users_to_add = rail.result('categorize_bulk_users')['users_to_add']

    # Deduplicate users by useruri
    unique_users = deduplicate_users_by_uri(users_to_add, 'useruri')

    batches = []
    for i in range(0, len(unique_users), batch_size):
        batch = unique_users[i:i + batch_size]
        batches.append({
            'user_uris': [user['useruri'] for user in batch],
            'users': batch
        })

    return batches


def batch_users_for_date_range_update(batch_size=50):
    """
    Batch users for CreateProjectOrApplyModifications API calls (date range updates).
    Includes all users that need date range updates (both new and existing).
    Deduplicates users by useruri to avoid processing same user multiple times.
    Returns list of batches, each containing up to batch_size users.
    """
    users_for_date_range = rail.result('categorize_bulk_users')['users_for_date_range_update']

    # Deduplicate users by useruri
    unique_users = deduplicate_users_by_uri(users_for_date_range, 'useruri')

    batches = []
    for i in range(0, len(unique_users), batch_size):
        batch = unique_users[i:i + batch_size]
        batches.append({
            'users': batch
        })

    return batches


# Child WBS batching functions

def batch_users_for_child_assignment(batch_size=50):
    """
    Batch users for BulkUpdateProjectTeamMembersAssignment API calls on child WBS.
    Only includes users_to_add (new users not yet assigned to child project).
    Deduplicates users by useruri to avoid processing same user multiple times.
    """
    users_to_add = rail.result('categorize_child_wbs_users')['users_to_add']

    # Deduplicate users by useruri
    unique_users = deduplicate_users_by_uri(users_to_add, 'useruri')

    batches = []
    for i in range(0, len(unique_users), batch_size):
        batch = unique_users[i:i + batch_size]
        batches.append({
            'user_uris': [user['useruri'] for user in batch],
            'users': batch
        })

    return batches


def batch_users_for_child_date_range_update(batch_size=50):
    """
    Batch users for CreateProjectOrApplyModifications API calls on child WBS (date range updates).
    Includes all users that need date range updates.
    Deduplicates users by useruri to avoid processing same user multiple times.
    """
    users_for_date_range = rail.result('categorize_child_wbs_users')['users_for_date_range_update']

    # Deduplicate users by useruri
    unique_users = deduplicate_users_by_uri(users_for_date_range, 'useruri')

    batches = []
    for i in range(0, len(unique_users), batch_size):
        batch = unique_users[i:i + batch_size]
        batches.append({
            'users': batch
        })

    return batches


def build_date_range_payload_from_conf(conf):
    """
    Build payload for CreateProjectOrApplyModifications API from dag_run.conf.
    Used by the child DAG (process_date_range_child.py) to process a single batch
    for both parent and child WBS date range updates.

    Expected conf structure:
    - project_uri: The project URI (parent or child)
    - batch: Dict containing 'users' list
    """
    resources_to_add = []

    for user in conf['batch']['users']:
        resource_assignment = {
            "resource": {
                "user": {
                    "uri": user['useruri']
                }
            },
            "assignmentDateRange": {
                "startDate": get_replicon_date(user['assignmentStartDate']) if user.get('assignmentStartDate') else null,
                "endDate": get_replicon_date(user['assignmentEndDate']) if user.get('assignmentEndDate') else null,
            } if user.get('assignmentStartDate') or user.get('assignmentEndDate') else null
        }
        resources_to_add.append(resource_assignment)

    return {
        "target": {
            "uri": conf['project_uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "resourceProjectAssignmentModifications": {
                "resourcesToAdd": resources_to_add
            }
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def build_assignment_payload_from_conf(conf):
    """
    Build payload for BulkUpdateProjectTeamMembersAssignment API from dag_run.conf.
    Used by the child DAG (process_assignment_child.py) to process a single batch
    for both parent and child WBS user assignments.

    Expected conf structure:
    - project_uri: The project URI (parent or child)
    - batch: Dict containing 'user_uris' list
    """
    return {
        "projectUri": conf['project_uri'],
        "resourceUri": conf['batch']['user_uris'],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }