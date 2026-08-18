from datetime import datetime
from uuid import uuid4
from airflow.exceptions import AirflowException
from airflow.models import Variable
import rail
from dxctechnology.wf39_psa_resource_assignment_v4.utils import request_payload
from dxctechnology.wf39_psa_resource_assignment_v4.mapper.item_categories import item_categories_mapper

null =None
CONCAT_STRING_DELIMITER = "*^*^*"


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def expand_per_role(records):
    """Expand PERN+WBS level records to per-role records for 1:1 log mapping with feed file rows."""
    expanded = []
    for record in records:
        record_dict = dict(record) if not isinstance(record, dict) else record
        roles_str = str(record_dict.get('roles', '')) if record_dict.get('roles') else ''
        roles = [r for r in roles_str.split(CONCAT_STRING_DELIMITER) if r] if roles_str else []
        if not roles:
            expanded_record = record_dict.copy()
            expanded_record['roles'] = ''
            expanded.append(expanded_record)
        else:
            for role in roles:
                expanded_record = record_dict.copy()
                expanded_record['roles'] = role
                expanded.append(expanded_record)
    return expanded


def get_input_combined_list(billing_rates_wbs_task_id):
    all_billing_rates_wbs = get_data_from_document(
        rail.result(billing_rates_wbs_task_id))
    return list(
        map(lambda x: {
            'wbs': x['wbs'],
            'role': x['role'],
            'startdate': datetime.strptime(x['startdate'], '%Y-%m-%d').strftime("%m/%d/%Y") if x['startdate'] else "",
            'enddate': datetime.strptime(x['enddate'], '%Y-%m-%d').strftime("%m/%d/%Y") if x['enddate'] else "",
            'employeeid': x['employeeid']
        }, all_billing_rates_wbs)
    )




def project_status(get_project_info):
    value = rail.result(get_project_info)['status']['name']
    if value not in ['Completed', 'Archived', 'Cancelled']:
        return True
    return False


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

# pylint: disable=too-many-return-statements


def is_project_c1():
    data = rail.result("get_division_detail")
    if data['code'] == "C1":
        return True
    return False



def check_can_assign_labour_type():
    project_item_category = rail.find_first_by_attr_and_get_attr(
        rail.result("get_project_info_from_project_service")["extensionFieldValues"],
            "tag.definition.displayText", "Item Category", "tag.displayText")
    return project_item_category in list(map(lambda mapper: mapper["item_category"],
        filter(lambda mapper: mapper["validate"].lower() == "yes", item_categories_mapper)))

def is_assignment_out_of_range_from_wbs_dates(assignment_start_date,assignment_end_date,wbs_start_date,wbs_end_date):
    if assignment_start_date and wbs_start_date:
        if assignment_start_date < wbs_start_date:
            return True
    if assignment_end_date and wbs_end_date:
        if assignment_end_date > wbs_end_date:
            return True
    return False

def is_user_out_of_range(assignment_start_date,assignment_end_date, user_start_date, user_end_date):
    if assignment_start_date and user_start_date:
        if assignment_start_date < user_start_date:
            return True
    if assignment_end_date and user_end_date:
        if assignment_end_date > user_end_date:
            return True
    return False

def validate_wbs_user_start_end_date_with_assignment_dates(dag_run, item):
    assignment_start_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        item['min_start_date'])) if item['min_start_date'] else ""
    assignment_end_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        item['max_end_date'])) if item['max_end_date'] else ""
    
    wbs_start_date = get_date_from_replicon_date(rail.result("get_project_info_from_project_service")["timeEntryDateRange"]["startDate"]) if rail.result(
        "get_project_info_from_project_service")["timeEntryDateRange"]["startDate"] else ""
    wbs_end_date = get_date_from_replicon_date(rail.result("get_project_info_from_project_service")["timeEntryDateRange"]["endDate"]) if rail.result(
        "get_project_info_from_project_service")["timeEntryDateRange"]["endDate"] else ""
    wbs_dates_check = is_assignment_out_of_range_from_wbs_dates(assignment_start_date,assignment_end_date,wbs_start_date,wbs_end_date)
    
    user_start_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        item['user_start_date'], format="%d %B %Y")) if item['user_start_date'] else ""

    user_end_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        item['user_end_date'], format="%d %B %Y")) if item['user_end_date'] else ""
    
    user_dates_check = is_user_out_of_range(assignment_start_date,assignment_end_date, user_start_date, user_end_date)

    if wbs_dates_check:
        return True, 'Labour Type is Not assigned to user due to Assignment start/end date outside the WBS start/end date'
    if user_dates_check:
        return True, 'Labour Type is Not assigned to user due to Assignment start/end date outside the user start/end date'
    
    return False, None

def get_assignement_dates_validation(dag_run):
    log_items = []
    records_to_process = []
    get_all_report_feed_data = rail.load_all_records(rail.result('query_all_records_for_wbs'))
    for item in get_all_report_feed_data:
        should_log, log_message = validate_wbs_user_start_end_date_with_assignment_dates(dag_run, item)
        if should_log:
            # Expand per role for 1:1 log mapping with feed file rows
            roles_str = str(item.get('roles', '')) if item.get('roles') else ''
            roles = [r for r in roles_str.split(CONCAT_STRING_DELIMITER) if r] if roles_str else ['']
            for role in roles:
                log_items.append({
                    'employeeid': item['employeeid'],
                    'wbs': item['wbs'],
                    'role': role,
                    'status': 'Exception',
                    'action': 'Validation',
                    'log_message': log_message
                })
        else:
            records_to_process.append(item)

    rail.set_result(key='records_to_process', val=records_to_process)
    return log_items

def validate_project_checks():
    if not is_project_c1():
        return False
    if not bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_info_from_project_service')[
        'extensionFieldValues'], "definition.displayText", "PSA Flag", "tag", False)):
        return False
    if bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_info_from_project_service')[
        'extensionFieldValues'], "definition.displayText", "Parent WBS", "textValue", False)):
        return False
    if not bool(rail.result('get_project_info_from_project_service')):
        return False
    if not project_status('get_project_info_from_project_service'):
        return False
    return True

def get_log_message_project_validations(item):
    if not is_project_c1():
        return 'WBS Element is not C1'
    if not bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_info_from_project_service')[
        'extensionFieldValues'], "definition.displayText", "PSA Flag", "tag", False)):
        return 'WBS Element is not PSA'
    if bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_info_from_project_service')[
        'extensionFieldValues'], "definition.displayText", "Parent WBS", "textValue", False)):
        return 'WBS Element is IWO'
    if not bool(rail.result('get_project_info_from_project_service')):
        return f"Required WBS {item['wbs']} is not available in Replicon"
    if not project_status('get_project_info_from_project_service'):
        return f"Required WBS {item['wbs']} is in { rail.result('get_project_info_from_project_service')['status']['name'] } status in Replicon"
    raise AirflowException('Record went for invalid even though all the mandatory field are present')

def get_billing_rates(item,dag_run):
    if not check_can_assign_labour_type():
        return []

    labor_type_to_assign = []
    feed_labor_types = str(item['roles']).split(CONCAT_STRING_DELIMITER) if item['roles'] else []

    if not feed_labor_types:
        return []

    # Get the assigned labor types and all billing rates from Replicon
    assigned_labor_types = rail.result('get_all_assigned_labor_types_to_project')
    all_billing_rates = rail.load_all_records(dag_run.conf['billing_rates_from_replicon'])

    for role in feed_labor_types:
        if role:
            # Check for Billable and Non-Billable variants
            billable_variant = f"{role}|Billable"
            non_billable_variant = f"{role}|Non-Billable"

            # Find which variant exists in the project
            variant_to_use = None
            if billable_variant in assigned_labor_types:
                variant_to_use = billable_variant
            elif non_billable_variant in assigned_labor_types:
                variant_to_use = non_billable_variant

            if variant_to_use:
                # Find the matching billing rate with URI from all_billing_rates
                matching_rate = next(
                    (rate for rate in all_billing_rates
                     if rate.get('displayText') == variant_to_use or
                        rate.get('name') == variant_to_use),
                    None
                )

                if matching_rate and matching_rate.get('uri'):
                    labor_type_to_assign.append({
                        "uri": matching_rate['uri']
                    })
                else:
                    # Fallback to name if URI not found (shouldn't happen normally)
                    labor_type_to_assign.append({
                        "name": variant_to_use
                    })

    return labor_type_to_assign

def get_resources_to_add(dag_run):
    get_all_feed_data = rail.result("assignement_dates_validation","records_to_process")
    return list(map(lambda item: {
                "resource": {
                    "user": {
                        "uri": item['user_uri']
                    }
                },
                "billingRates": get_billing_rates(item,dag_run),
                "assignmentDateRange": {
                    "startDate": request_payload.get_replicon_date(item['min_start_date']) if item['min_start_date'] else null,
                    "endDate": request_payload.get_replicon_date(item['max_end_date']) if item['max_end_date'] else null,
                } if item['min_start_date'] or item['max_end_date'] else null
        }, get_all_feed_data))


def get_project_resource_assignment_payload(dag_run):
    resource_to_add = get_resources_to_add(dag_run)
    return {
        "target": {
            "uri": rail.result('get_project_info_from_project_service')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "resourceProjectAssignmentModifications": {
                "resourcesToAdd": resource_to_add
            }
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

# Enhanced methods for v4 integration

# ---------------------------------------------------------------------------
# Change-detection (idempotency) gate helpers
#
# These compare each feed record against the current assignment state already
# present in Replicon, so that records whose dates and labour types are
# unchanged are NOT re-applied. Skipping the no-op re-applications stops
# Replicon from publishing redundant modification webhooks, which is the root
# cause of the duplicate records being exported downstream to C1.
#
# The gate is controlled by an Airflow Variable whose name is supplied per
# instance (see config.idempotency_gate_var_name). When the variable name is
# None/unset or evaluates to "false", every record flows through the existing
# v4 path unchanged - i.e. this is a true no-op when disabled.
# ---------------------------------------------------------------------------

def is_idempotency_gate_enabled(gate_var_name):
    """Return True only when a gate Variable name is configured AND set to 'true'."""
    if not gate_var_name:
        return False
    return Variable.get(gate_var_name, default_var='false').lower() == 'true'


def _replicon_dates_equal(feed_date, current_date):
    """Compare two Replicon date dicts ({year, month, day}) at day granularity."""
    if not feed_date and not current_date:
        return True
    if not feed_date or not current_date:
        return False
    return (feed_date.get('year') == current_date.get('year')
            and feed_date.get('month') == current_date.get('month')
            and feed_date.get('day') == current_date.get('day'))


def is_record_unchanged(record, current_state, valid_roles):
    """
    A record is considered unchanged (already in sync in Replicon) when:
      - the user is already a team member on the project, AND
      - the feed assignment start/end dates match the current assignment dates, AND
      - every valid feed labour type is already assigned to that user.
    Records WITHOUT labour types (valid_roles empty) are unchanged when the user
    is present and the dates match.
    """
    user_uri = record.get('user_uri')
    if not user_uri or user_uri not in current_state:
        return False  # new team member -> must apply

    current = current_state[user_uri]

    feed_start = request_payload.get_replicon_date(record['min_start_date']) if record.get('min_start_date') else None
    feed_end = request_payload.get_replicon_date(record['max_end_date']) if record.get('max_end_date') else None

    if not _replicon_dates_equal(feed_start, current.get('startDate')):
        return False
    if not _replicon_dates_equal(feed_end, current.get('endDate')):
        return False

    current_labour_types = set(current.get('labour_types') or [])
    for role in valid_roles:
        if role and role.lower() not in current_labour_types:
            return False  # a feed labour type is not yet assigned -> must apply

    return True


def validate_and_filter_labor_types(dag_run, gate_var_name=None):
    """
    Enhanced validation that checks each labour type against project assignments
    Similar to v4 non-compass check_labour_type but for bulk processing.

    When the idempotency gate is enabled, records that are already in sync with
    Replicon (matching user, dates and labour types) are diverted to the
    `unchanged_records` bucket and excluded from the apply/modify batches.
    """
    all_records = rail.result("assignement_dates_validation", "records_to_process")
    assigned_labor_types = rail.result('get_all_assigned_labor_types_to_project')

    gate_enabled = is_idempotency_gate_enabled(gate_var_name)
    # `or {}` guards the case where the gate was toggled on after the upstream
    # get_current_team_assignments was skipped (result would be None) - we then
    # safely treat everything as changed rather than crash.
    current_state = (rail.result('get_current_team_assignments') or {}) if gate_enabled else {}
    # Whether labour types are actually applied for this project. If the item
    # category disables labour-type assignment, get_billing_rates() returns []
    # and only the date range is applied - so the idempotency check must ignore
    # the feed roles (compare as if the record had no role).
    labour_types_assignable = check_can_assign_labour_type() if gate_enabled else False

    valid_records_with_labor_type_present_in_project = []
    valid_records_without_labor_type = []
    records_with_labor_type_not_present_in_project = []
    log_valid_records_with_labor_type = []  # per-role for 1:1 success logging
    unchanged_records = []  # per-role expanded, for NoChange logging back to PSA

    for record in all_records:
        if record.get('roles'):
            # Split concatenated roles
            roles = str(record['roles']).split(CONCAT_STRING_DELIMITER) if record['roles'] else []
            valid_roles = []
            invalid_roles = []
            has_empty_role = False  # Track if this record has empty labor types mixed with others

            for role in roles:
                if role:
                    # Check if labour type exists in project (with Billable/Non-Billable variants)
                    billable_variant = f"{role}|Billable"
                    non_billable_variant = f"{role}|Non-Billable"

                    # Check if this labour type or its variants are already assigned
                    is_assigned = (billable_variant in assigned_labor_types or
                                   non_billable_variant in assigned_labor_types)

                    if is_assigned:
                        valid_roles.append(role)
                    else:
                        invalid_roles.append(role)
                else:
                    # Empty role found within concatenated roles
                    has_empty_role = True

            # Create records for valid and invalid labour types
            if valid_roles:
                # Compare against the roles the integration actually applies:
                # none when labour-type assignment is disabled for this project.
                effective_roles = valid_roles if labour_types_assignable else []
                if gate_enabled and is_record_unchanged(record, current_state, effective_roles):
                    # Already in sync in Replicon - skip re-apply, log as NoChange
                    for role in valid_roles:
                        unchanged_record = record.copy()
                        unchanged_record['roles'] = role
                        unchanged_records.append(unchanged_record)
                else:
                    valid_record = record.copy()
                    valid_record['roles'] = CONCAT_STRING_DELIMITER.join(valid_roles)
                    valid_records_with_labor_type_present_in_project.append(valid_record)
                    # Per-role log entries for 1:1 success log mapping
                    for role in valid_roles:
                        log_record = record.copy()
                        log_record['roles'] = role
                        log_valid_records_with_labor_type.append(log_record)

            # If record has empty roles mixed with other roles, also add to without_labor_type list
            if has_empty_role:
                # Create a special record for tracking mixed empty labor types
                empty_role_record = record.copy()
                empty_role_record['roles'] = ''  # Mark as empty
                # The empty-role portion only applies user + date range (no rate),
                # so idempotency for it is purely user-on-team + dates-match.
                if gate_enabled and is_record_unchanged(record, current_state, []):
                    unchanged_records.append(empty_role_record)
                else:
                    empty_role_record['has_mixed_empty_role'] = True  # Flag to indicate this had empty + other roles
                    empty_role_record['original_roles'] = record.get('roles')  # Preserve original for reference
                    valid_records_without_labor_type.append(empty_role_record)

            if invalid_roles:
                for invalid_role in invalid_roles:
                    # Don't create exception records for empty roles
                    if invalid_role:  # Only create exception for non-empty invalid roles
                        invalid_record = {
                            'employeeid': record['employeeid'],
                            'wbs': record['wbs'],
                            'roles': invalid_role,
                            'status': 'Exception',
                            'action': 'Validation'
                        }
                        records_with_labor_type_not_present_in_project.append(invalid_record)
        else:
            # Handle records without labor types - include them in valid_records
            # These users will be assigned to the project without specific billing rates
            if gate_enabled and is_record_unchanged(record, current_state, []):
                # Already in sync in Replicon - skip re-apply, log as NoChange
                unchanged_record = record.copy()
                unchanged_record['roles'] = ''
                unchanged_records.append(unchanged_record)
            else:
                valid_record = record.copy()
                valid_record['roles'] = ''  # Keep roles as empty string
                valid_record['has_mixed_empty_role'] = False  # Flag to indicate this is purely empty
                valid_records_without_labor_type.append(valid_record)

    # Set results for different paths
    rail.set_result(key='valid_records_with_labor_type_present_in_project', val=valid_records_with_labor_type_present_in_project)
    rail.set_result(key='records_with_labor_type_not_present_in_project', val=records_with_labor_type_not_present_in_project)
    rail.set_result(key='valid_records_without_labor_type', val=valid_records_without_labor_type)
    rail.set_result(key='log_valid_records_with_labor_type', val=log_valid_records_with_labor_type)
    rail.set_result(key='unchanged_records', val=unchanged_records)

    return {
        'valid_records_with_labor_type_present_in_project': valid_records_with_labor_type_present_in_project,
        'records_with_labor_type_not_present_in_project': records_with_labor_type_not_present_in_project,
        'valid_records_without_labor_type': valid_records_without_labor_type,
        'log_valid_records_with_labor_type': log_valid_records_with_labor_type,
        'unchanged_records': unchanged_records
    }

def get_project_resource_assignment_payload_enhanced(records,action,dag_run):
    """
    Enhanced version that only processes valid records with/without labour types
    """
    resources_to_add = []

    for item in records:
        billing_rates = get_billing_rates(item,dag_run) if action == "with_labour_types" else []

        if billing_rates or (item.get('min_start_date') or item.get('max_end_date')):
            resource_assignment = {
                "resource": {
                    "user": {
                        "uri": item['user_uri']
                    }
                },
                "billingRates": billing_rates,
                "assignmentDateRange": {
                    "startDate": request_payload.get_replicon_date(item['min_start_date']) if item.get('min_start_date') else null,
                    "endDate": request_payload.get_replicon_date(item['max_end_date']) if item.get('max_end_date') else null,
                } if item.get('min_start_date') or item.get('max_end_date') else null
            }
            resources_to_add.append(resource_assignment)

    return {
        "target": {
            "uri": rail.result('get_project_info_from_project_service')['uri'],
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

def batch_resources_for_assignment_with_labor_types(batch_size=50):
    """
    Batch resources WITH labor types for BulkUpdateProjectTeamMembersAssignment API calls.
    Returns list of batches, each containing up to batch_size resource URIs.
    """
    records = rail.result("validate_labor_types_in_project", "valid_records_with_labor_type_present_in_project")

    batches = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batches.append({
            'resource_uris': [record['user_uri'] for record in batch],
            'records': batch
        })
    return batches


def batch_resources_for_assignment_without_labor_types(batch_size=50):
    """
    Batch resources WITHOUT labor types for BulkUpdateProjectTeamMembersAssignment API calls.
    Returns list of batches, each containing up to batch_size resource URIs.
    """
    records = rail.result("validate_labor_types_in_project", "valid_records_without_labor_type")

    batches = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batches.append({
            'resource_uris': [record['user_uri'] for record in batch],
            'records': batch
        })
    return batches


def build_bulk_assignment_payload(batch):
    """
    Build payload for BulkUpdateProjectTeamMembersAssignment API.
    """
    return {
        "projectUri": rail.result('get_project_info_from_project_service')['uri'],
        "resourceUri": batch['resource_uris'],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }


def batch_resources_for_modification_with_labor_types(batch_size=50):
    """
    Batch resources WITH labor types for CreateProjectOrApplyModifications API calls.
    Returns list of batches, each containing up to batch_size resources.
    """
    records = rail.result("validate_labor_types_in_project", "valid_records_with_labor_type_present_in_project")

    batches = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batches.append({
            'records': batch,
            'action': 'with_labour_types'
        })
    return batches


def batch_resources_for_modification_without_labor_types(batch_size=50):
    """
    Batch resources WITHOUT labor types for CreateProjectOrApplyModifications API calls.
    Returns list of batches, each containing up to batch_size resources.
    """
    records = rail.result("validate_labor_types_in_project", "valid_records_without_labor_type")

    batches = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batches.append({
            'records': batch,
            'action': 'without_labour_types'
        })
    return batches


def build_project_modification_payload_for_batch(batch, dag_run):
    """
    Build payload for CreateProjectOrApplyModifications API for a batch.
    """
    records = batch['records']
    action = batch['action']
    resources_to_add = []

    for item in records:
        billing_rates = get_billing_rates(item, dag_run) if action == "with_labour_types" else []

        if billing_rates or (item.get('min_start_date') or item.get('max_end_date')):
            resource_assignment = {
                "resource": {
                    "user": {
                        "uri": item['user_uri']
                    }
                },
                "billingRates": billing_rates,
                "assignmentDateRange": {
                    "startDate": request_payload.get_replicon_date(item['min_start_date']) if item.get('min_start_date') else null,
                    "endDate": request_payload.get_replicon_date(item['max_end_date']) if item.get('max_end_date') else null,
                } if item.get('min_start_date') or item.get('max_end_date') else null
            }
            resources_to_add.append(resource_assignment)

    return {
        "target": {
            "uri": rail.result('get_project_info_from_project_service')['uri'],
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


def do_format_logs():
    log_artifacts = []
    log_records = []

    master_log = rail.result("create_master_log")
    if master_log:
        if isinstance(master_log, list):
            log_artifacts.extend(master_log)
        else:
            log_artifacts.append(master_log)

    logs = rail.result("gather_process_billing_rates_logs")

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

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **log['properties'],
        'message': log['message'],
        'ecid': log['ecid']
        }, log_records))

    rail.set_result(key="get_successful_billing_rates", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_errored_billing_rates", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_exception_billing_rates", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))
    rail.set_result(key="get_skipped_billing_rates", val=len(list(filter(lambda item: item['status']=="Skipped", final_log_records))))
    print(final_log_records)

    return final_log_records

