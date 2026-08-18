from datetime import datetime
from uuid import uuid4
import rail
from dxctechnology.wf39_psa_resource_assignment_compass_v3.utils import request_payload
from airflow.exceptions import AirflowException
from rail.lib.log import get_master_log_artifact_name

null = None
CONCAT_STRING_DELIMITER = "*^*^*"

def project_status(get_project_info):
    value = rail.result(get_project_info)['status']['name']
    if value not in ['Completed', 'Archived', 'Cancelled']:
        return True
    return False

def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def assignedBillingRates(get_all_project_team_assignment):
    return rail.result(get_all_project_team_assignment)['user_data'][0]['billingRatesAllowedForBillingTime']

def is_project_compass():
    data = rail.result("get_division_detail")
    if data['code'] == "COMPASS":
        return True
    return False

def check_can_assign_labour_type():
    compass_t_m_indicator = rail.find_first_by_attr_and_get_attr(
        rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["extensionFieldValues"],
            "tag.definition.displayText", "COMPASS T&M Indicator", "tag.displayText")
    return compass_t_m_indicator == 'X'

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

    return final_log_records

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

def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

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
            log_items.append({
                'employeeid': item['employeeid'],
                'wbs': item['wbs'],
                'role': item['roles'],
                'status': 'Exception',
                'action': 'Validation',
                'log_message': log_message
            }
            )
        else :
            records_to_process.append(item)

    rail.set_result(key='records_to_process', val=records_to_process)
    return log_items

def validate_project_checks():
    if not is_project_compass():
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
    if not is_project_compass():
        return 'WBS Element is not Compass'
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

def get_billing_rates(item):
    if rail.find_first_by_attr_and_get_attr(
        rail.result("get_project_info_from_project_service")["extensionFieldValues"],
            "tag.definition.displayText", "COMPASS T&M Indicator", "tag.displayText") != "X":
        return []
    labor_type_to_assign = []
    feed_labor_types = str(item['roles']).split(CONCAT_STRING_DELIMITER) if item['roles'] else []
    
    if not feed_labor_types:
        return []

    if feed_labor_types:
        for item in feed_labor_types:
            if item:
                labor_type_to_assign.append(
                    {
                        "name": item
                    }
                )
    return labor_type_to_assign

def get_resources_to_add(dag_run):
    get_all_feed_data = rail.result("assignement_dates_validation","records_to_process")
    return list(map(lambda item: {
                "resource": {
                    "user": {
                        "uri": item['user_uri']
                    }
                },
                "billingRates": get_billing_rates(item),
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

def validate_project_and_division():
    if rail.result('get_project_info_from_project_service'):
        if rail.result('get_project_info_from_project_service')['division']:
            return True
    return False


def batch_resources_for_assignment(batch_size=50):
    """
    Batch resource URIs for BulkUpdateProjectTeamMembersAssignment API calls.
    Returns list of batches, each containing up to batch_size resource URIs.
    """
    records_to_process = rail.result("assignement_dates_validation", "records_to_process")

    batches = []
    for i in range(0, len(records_to_process), batch_size):
        batch = records_to_process[i:i + batch_size]
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


def batch_resources_for_modification(batch_size=50):
    """
    Batch resources for CreateProjectOrApplyModifications API calls.
    Returns list of batches, each containing up to batch_size resources with billing rates and date ranges.
    """
    records_to_process = rail.result("assignement_dates_validation", "records_to_process")

    batches = []
    for i in range(0, len(records_to_process), batch_size):
        batch = records_to_process[i:i + batch_size]
        batches.append({
            'records': batch
        })

    return batches


def build_resources_to_add_for_batch(records):
    """
    Build resourcesToAdd array for a batch of records.
    """
    return list(map(lambda item: {
        "resource": {
            "user": {
                "uri": item['user_uri']
            }
        },
        "billingRates": get_billing_rates(item),
        "assignmentDateRange": {
            "startDate": request_payload.get_replicon_date(item['min_start_date']) if item['min_start_date'] else null,
            "endDate": request_payload.get_replicon_date(item['max_end_date']) if item['max_end_date'] else null,
        } if item['min_start_date'] or item['max_end_date'] else null
    }, records))


def build_project_modification_payload_for_batch(batch):
    """
    Build payload for CreateProjectOrApplyModifications API for a batch.
    """
    resources_to_add = build_resources_to_add_for_batch(batch['records'])
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

def get_project_division_check_message():
    if not rail.result('get_project_info_from_project_service'): 
        return 'WBS Element is not present in Replicon'
    if not rail.result('get_project_info_from_project_service')['division']:
        return 'WBS Element does not have division associated in Replicon'
    raise AirflowException('Record went for invalid even though all the mandatory field are present')
