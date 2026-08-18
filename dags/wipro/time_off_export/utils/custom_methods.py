from datetime import datetime as dt, timedelta, timezone
import rail
from airflow.models import Variable, DagRun
from wipro.time_off_export.mapper.timeoff_mapper import ABSENCE_MAPPER

DATE_FORMAT = "%Y-%m-%d"

event_mapper = {
    "approved": "Approved",
    "rejected": "Rejected",
    "reopen": 'Reopen',
    "waiting": 'Waiting For Approval'
}

def get_event(dag_run):
    if dag_run.conf['data'].get("owner"):
        return "Deleted"
    return event_mapper[dag_run.conf['data']['timeOffStatusUri'].split(":")[-1]]

def get_converted_date(_date,_format):
    return dt.strptime(_date,_format).strftime(DATE_FORMAT)

def get_day_diff(start_date,end_date):
    daydiff = dt.strptime(end_date, DATE_FORMAT) - dt.strptime(start_date, DATE_FORMAT)
    return daydiff.days + 1

def truncate_manager_id(employee_id):
    if not employee_id:
        return ""
    return employee_id.split("_")[0][:8]

def get_payload_to_submit(dag_run):
    is_delete_event = bool(get_event(dag_run) == 'Deleted')
    timeoff_data = rail.result("get_non_deleted_timeoff_details") if not is_delete_event else rail.result("get_deleted_timeoff_details")

    return {
        "d": {
            "EmployeeId": rail.result("get_user_details")['employee_id'],
            "WorkItemId": dag_run.conf['data']['timeOff']['uri'].split(":")[-1],
            "LeaveStartDate": get_converted_date(timeoff_data['start_date'],'%Y/%m/%d'),
            "LeaveEndDate": get_converted_date(timeoff_data['end_date'],'%Y/%m/%d'),
            "AbsenceTypeCode": rail.find_first_by_attr_and_get_attr(ABSENCE_MAPPER,'Name',timeoff_data['absence_type_text'],'Code', ""),
            "AbsenceTypeText": timeoff_data['absence_type_text'],
            "AbsencePaidStatus": rail.find_first_by_attr_and_get_attr(ABSENCE_MAPPER,'Name',timeoff_data['absence_type_text'],'Paid', ""),
            "HolidayStatus": "HOLIDAY" if timeoff_data[
                'absence_type_text'] == "Bank Holiday" else "",
            "EmployeeName": rail.result("get_user_details")['name'],
            "Country": rail.result("get_country_details")['code'],
            "RequestedDate": get_converted_date(rail.result("get_approval_history_details")['date'],'%Y/%m/%d') if not is_delete_event else '',
            "RequestedTime": rail.result("get_approval_history_details")['time'] if not is_delete_event else '',
            "StartTime": timeoff_data['start_time'],
            "EndTime": timeoff_data['end_time'],
            "NoOfDays": timeoff_data['total_hours'],
            "ApprovalStatus": get_event(dag_run),
            "Reason": timeoff_data['comments'] if not is_delete_event and timeoff_data['comments'] else '',
            "ManagerId": truncate_manager_id(rail.result("get_manager_details_in_replicon")['employee_id']),
            "ManagerName": rail.result("get_manager_details_in_replicon")['name'],
            "UpdatedOn": get_converted_date(dag_run.conf['received_at'][:10],"%Y-%m-%d"),
            "UpdatedBy": rail.result("get_acting_user_empid_in_replicon") if rail.result(
                "get_acting_user_empid_in_replicon") else rail.result("get_user_details")['employee_id']
        }
    }

def get_submit_child_conf(dag_run):
    return {
        "data": get_payload_to_submit(dag_run),
        "employee_id": rail.result("get_user_details")['employee_id'],
        "work_item_iD": dag_run.conf['data']["timeOff"]["uri"],
        "booking_start_date": rail.result("get_non_deleted_timeoff_details")['start_date'] if rail.result(
            "get_non_deleted_timeoff_details") else rail.result("get_deleted_timeoff_details")['start_date'],
        "booking_end_date": rail.result("get_non_deleted_timeoff_details")['end_date'] if rail.result(
            "get_non_deleted_timeoff_details") else rail.result("get_deleted_timeoff_details")['end_date'],
        "event": get_event(dag_run),
        'log': rail.result("create_log")
    }

def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):

    current_time = dt.now(timezone.utc)
    lookup_timestamp_value = Variable.get(
        lookup_log_timestamp_var, default_var=None)

    query_execution_start_date = dt.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
        current_time - timedelta(hours=lookup_log_timestamp_hours))

    dag_runs = []
    execution_dates = []
    for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
        execution_dates.append(run.execution_date)
        dag_runs.append(run.id)
    if execution_dates:
        max_execution_date = max(execution_dates)
        Variable.set(lookup_log_timestamp_var,
                     (max_execution_date + timedelta(seconds=1)).isoformat())
    return dag_runs

def catch_and_log_errors(dag_run, **context):
    """
    Checks if the error should be logged to customer.
    Returns:
    - {"should_log": False, "properties": None} if URI not found (deleted timeoff)
    - {"should_log": True, "properties": {...}} for genuine errors
    """
    # Get the full error dictionary from XCom to check detailed error info
    failed_task_ids = [ti.task_id for ti in dag_run.get_task_instances(state='failed')]
    error_dict = None
    error_message = ''

    if failed_task_ids:
        error_dict = context['ti'].xcom_pull(failed_task_ids[0], key='error')

        # Build the error message for logging (same logic as get_error_message() macro)
        if isinstance(error_dict, dict):
            # Try to get response body first, then exc_message, then the whole dict
            response = error_dict.get('response', {})
            if isinstance(response, dict):
                error_message = response.get('body') or error_dict.get('exc_message') or str(error_dict)
                if isinstance(response.get('json'), dict) and response.get('json').get('error',{}).get('reason',''):
                    error_message = response['json']['error']['reason']
            else:
                error_message = error_dict.get('exc_message') or str(error_dict)
        else:
            error_message = str(error_dict) if error_dict else 'Unknown error occurred'

    # Check if this is a "URI not found" error (deleted timeoff)
    # This happens when timeoff is deleted in Replicon before the integration processes it
    is_uri_not_found_error = False
    correlation_id = ''

    if error_dict and isinstance(error_dict, dict):
        exc_message = error_dict.get('exc_message', '')
        response = error_dict.get('response', {})

        # Check for 400 Bad Request with URI not found in response body
        if '400 Bad Request' in exc_message or (isinstance(response, dict) and response.get('status_code') == 400):
            if isinstance(response, dict):
                body = response.get('body', '')
                response_json = response.get('json', {})

                # Check in JSON response if available
                if isinstance(response_json, dict):
                    error_info = response_json.get('error', {})
                    if isinstance(error_info, dict):
                        reason = error_info.get('reason', '')
                        error_type = error_info.get('type', '')
                        if 'URI not found' in reason or 'Incorrect URI Type' in reason or error_type == 'UriError1':
                            is_uri_not_found_error = True

                # Also check in body string as fallback
                if not is_uri_not_found_error and isinstance(body, str):
                    if 'URI not found' in body or 'Incorrect URI Type' in body:
                        is_uri_not_found_error = True

        # Extract SAP correlation ID stored by CustomSimpleHttpOperator2 via XCom
        correlation_id = context['ti'].xcom_pull(failed_task_ids[0], key='sap_correlation_id') or ''

    if correlation_id:
        error_message = f"{error_message} | SAP Correlation ID: {correlation_id}"

    if is_uri_not_found_error:
        # Deleted timeoff - don't log to customer
        return {
            "should_log": False,
            "properties": None
        }
    else:
        # Genuine error - prepare properties for logging
        return {
            "should_log": True,
            "properties": {
                "employee_id": rail.result("get_user_details")['employee_id'] if rail.result("get_user_details") else '',
                "work_item_id": dag_run.conf['data']["timeOff"]["uri"],
                "booking_start_date": rail.result("get_non_deleted_timeoff_details")['start_date'] if rail.result(
                    "get_non_deleted_timeoff_details") else (rail.result("get_deleted_timeoff_details")['start_date'] if rail.result("get_deleted_timeoff_details") else ''),
                "booking_end_date": rail.result("get_non_deleted_timeoff_details")['end_date'] if rail.result(
                    "get_non_deleted_timeoff_details") else (rail.result("get_deleted_timeoff_details")['end_date'] if rail.result("get_deleted_timeoff_details") else ''),
                "event": get_event(dag_run),
                "status": "Error",
                "response": error_message
            }
        }

def do_format_logs():
    final_log_records = []
    logs = rail.result("get_timeoff_export_logs")
    for log in logs:
        final_log_records.extend(rail.load_all_records(log))
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['status'] == 'Success', final_log_records))))
    return rail.write_json_artifact(final_log_records)
