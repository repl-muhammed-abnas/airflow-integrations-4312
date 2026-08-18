from datetime import datetime
from functools import lru_cache
from itertools import chain
import pendulum
import rail

def logging_details(time_zone):
    today = pendulum.now(time_zone)
    return {
        "current_time_json": {
            "year": today.year,
            "month": today.month,
            "day": today.day
        }
    }

def get_email_log_details(STANDARD_EMAIL_DATE_FORMAT):
    current_time = pendulum.now()
    start_time_str = rail.result("get_lastsync_time_and_current_time")["process_start_time"]
    return {
        "job_start_time": start_time_str,
        "job_end_time": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).total_seconds() / 60, 1),
        "log_file_name": rail.result("get_log_filename"),
        "log_file_link": rail.result("generate_log_file_link"),
        "total_record_count": len(rail.load_all_records(rail.result("bamboohr_updated_employees_data")))
    }

def generate_user_records_query(required_employee_fields, query_type="invalid", table_name="bamboohr_users_data"):
    mandatory_fields = [
        field["field_attr"] for field in required_employee_fields
        if field["mandatory"]
    ]
    if query_type == "valid":
        conditions = [f'NULLIF("{field}","") IS NOT NULL' for field in mandatory_fields]
        operator = " AND "
    else:
        conditions = [f'NULLIF("{field}","") IS NULL' for field in mandatory_fields]
        operator = " OR "
    where_clause = operator.join(conditions)
    return f'SELECT * FROM {table_name} WHERE {where_clause}'

def get_invalid_user_log_details(item):
    required_employee_fields = rail.result('filter_required_employee_fields')
    message = "User not processed due to following reason/s: "
    blank_fields = [field["bamboohr_name"] for field in required_employee_fields
        if field["mandatory"] and not item.get(field["field_attr"])]
    return message + "; ".join(blank_fields) + " not present in the payload"

def get_required_location(response, dag_run):
    return [{
        "location_name": location_data["displayText"],
        "uri": location_data["uri"]
    } for location_data in response if location_data["displayText"] == dag_run.conf["user_details"]["location"]]

def get_required_department(response, dag_run):
    return [{
        "department_name": department_data["displayText"],
        "uri": department_data["uri"]
    } for department_data in response if department_data["displayText"] == dag_run.conf["user_details"]["department"]]

def get_required_division(response, dag_run):
    return [{
        "division_name": division_data["displayText"],
        "uri": division_data["uri"]
    } for division_data in response if division_data["displayText"] == dag_run.conf["user_details"]["subsidiary"]]

def get_user_current_holiday_calendar(response):
    if not response:
        return {}
    return {
        "holiday_calendar_name": response[0]["holidayCalendar"]["displayText"],
        "uri": response[0]["holidayCalendar"]["uri"]
    }

def should_skip_update():
   modifications = rail.result("get_update_payload").get("modifications", {})
   return not any([
       modifications.get("displayName"),
       modifications.get("emailAddress"),
       modifications.get("employmentDateRange"),
       modifications.get("locationSchedule"),
       modifications.get("departmentGroupSchedule"),
       modifications.get("costCenterSchedule"),
       modifications.get("employeeTypeGroupSchedule"),
       modifications.get("serviceCenterSchedule"),
       modifications.get("divisionSchedule"),
       modifications.get("supervisorSchedule"),
       modifications.get("holidayCalendarSchedule"),
       modifications.get("extensionFields"),
       modifications.get("projectRoleSchedule"),
       modifications.get("timeOffTypes"),
       modifications.get("timeZone"),
       modifications.get("timesheetTemplate"),
       modifications.get("payRuleSchedule"),
       modifications.get("punchEntryPolicy"),
       modifications.get("products"),
       modifications.get("scheduleTypeSchedule"),
       modifications.get("securitySettings")
   ])

def do_format_logs():
    # Process supervisor logs separately first
    supervisor_logs = rail.result("gather_supervisor_logs") if rail.result("gather_supervisor_logs") else []
    supervisor_records = {}
    
    if supervisor_logs:
        for log in supervisor_logs:
            records = load_records_cached(log)
            for record in records:
                employee_id = record['properties'].get('employeeid', '')
                if employee_id:  # Only add to supervisor_records if employee_id is not blank
                    supervisor_records[employee_id] = {
                        **record['properties'],
                        "ecid": record['ecid']
                    }
    
    # Now process user logs and groups log
    log_artifacts = []
    log_records = []
    
    user_logs = rail.result("gather_user_logs") if rail.result("gather_user_logs") else []
    groups_log = rail.result("create_groups_log")
    
    logs = user_logs + [groups_log]
    
    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)
    
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records_cached(log)
            if each_log_records:
                log_records.extend(each_log_records)
    
    # Process and merge logs
    final_log_records = []
    processed_employees = set()
    
    # Status priority: Error > Exception > Success > Skipped
    status_priority = {"Error": 1, "Exception": 2, "Success": 3, "Skipped": 4}
    
    for log in log_records:
        employee_id = log['properties'].get('employeeid', '')
        
        # For blank employee IDs, include them directly without merging
        if not employee_id:
            final_log_records.append({
                **log['properties'],
                "ecid": log['ecid']
            })
            continue
            
        processed_employees.add(employee_id)
        
        # Check if there's a supervisor log for this employee
        if employee_id in supervisor_records:
            supervisor_log = supervisor_records[employee_id]
            user_log = {
                **log['properties'],
                "ecid": log['ecid']
            }
            
            # Compare status priorities
            user_priority = status_priority.get(user_log['status'], 5)
            supervisor_priority = status_priority.get(supervisor_log['status'], 5)
            
            if supervisor_priority < user_priority:
                # Supervisor log has higher priority
                merged_details = f"{user_log['details']} | {supervisor_log['details']}"
                final_log_records.append({
                    **supervisor_log,
                    "details": merged_details
                })
            else:
                # User log has higher or equal priority
                merged_details = f"{user_log['details']} | {supervisor_log['details']}"
                final_log_records.append({
                    **user_log,
                    "details": merged_details
                })
        else:
            # No supervisor log for this employee
            final_log_records.append({
                **log['properties'],
                "ecid": log['ecid']
            })
    
    # Add any supervisor logs for employees not in user logs
    for employee_id, supervisor_log in supervisor_records.items():
        if employee_id not in processed_employees:
            final_log_records.append(supervisor_log)
    
    rail.set_result(key="get_logged_success", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_logged_errors", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_logged_exceptions", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))
    rail.set_result(key="get_logged_skipped", val=len(list(filter(lambda item: item['status']=="Skipped", final_log_records))))
    
    return final_log_records

def get_error_message():
    context = rail.get_current_context()
    failed_task_ids = rail.lib.errors.get_failed_task_ids(context)
    error_message = ''
    if failed_task_ids:
        error_key = (context['ti'].xcom_pull(
            failed_task_ids[0], key='error') or 'Unknown error occurred')
        error_message = (error_key.get("response").get("body") if error_key.get("response")
                         else error_key.get('exc_message')) if isinstance(error_key, dict) else error_key

    return error_message

@lru_cache(maxsize=128)
def load_records_cached(artifact_id):
    """
    Cached wrapper for rail.load_all_records to avoid multiple loads of the same artifact.
    """
    if not artifact_id:
        return []
    return rail.load_all_records(artifact_id)

def get_all_employee_numbers_from_payload(dag_run):
    """
    Extract all employee numbers from the payload data.
    """
    artifact_id = dag_run.conf.get("all_employee_numbers_in_payload")
    employee_numbers_data = load_records_cached(artifact_id) if artifact_id else []
    if not employee_numbers_data:
        return []

    employee_numbers = [record.get('employeenumber') for record in employee_numbers_data if record.get('employeenumber')]

    return employee_numbers


# Generic pagination helper functions for BambooHR API
def get_remaining_page_numbers(first_page_task_id):
    """Returns list of page numbers to fetch (2 to total_pages) if pagination is needed."""
    initial_response = rail.result(first_page_task_id)
    pagination = initial_response.get("pagination", {})
    total_pages = pagination.get("total_pages", 1)

    if total_pages > 1:
        return list(range(2, total_pages + 1))
    return []


def get_page_data(task_id, data_key):
    """Get data from the specified task response using the given key."""
    response = rail.result(task_id)
    return response.get(data_key, [])


def get_flattened_data(variable_name, wrap_key=None):
    """
    Flatten data list from a variable (handles nested lists from append operations).
    If wrap_key is provided, returns {wrap_key: flattened_list}, otherwise returns flattened_list.
    """
    result = rail.result(variable_name)
    data = result.get("value", [])
    flattened = list(chain.from_iterable(
        item if isinstance(item, list) else [item] for item in data
    ))
    return {wrap_key: flattened} if wrap_key else flattened
