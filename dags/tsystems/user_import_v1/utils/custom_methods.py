import rail
from datetime import datetime
import pendulum
from functools import lru_cache

null = None

@lru_cache(maxsize=128)
def get_cached_result(task_id):
    """LRU cached version of rail.result to avoid repeated DB lookups"""
    return rail.result(task_id)

@lru_cache(maxsize=128)
def cached_load_all_records(artifact_id):
    """LRU cached version of rail.load_all_records to avoid reloading the same artifacts"""
    return rail.load_all_records(artifact_id)


def get_all_user_employee_ids_from_feed(dag_run):
    """Get all employee IDs from the current import feed"""
    artifact_id = dag_run.conf.get("all_users_employee_ids")
    if not artifact_id:
        return []
    all_users = cached_load_all_records(artifact_id)
    # Extract just the employeeid values from the query result
    return [user.get("employeeid") for user in all_users if user.get("employeeid")]

def create_users_payload_from_variable(item, config):
    """
    Create users payload from the variable set in the DAG run configuration.
    This is used to create the initial payload for user import.
    """
    # First merge the business logic results
    merged_item = {
        **item,
        **apply_tsystems_business_logic(item, config)
    }
    
    # Get the groups data
    orgstsructure_group_data = cached_load_all_records(get_cached_result("get_location_group_as_orgstructure"))
    departments_group_data = cached_load_all_records(get_cached_result("get_servicecenter_as_department"))
    costcenters_group_data = cached_load_all_records(get_cached_result("get_department_as_costcenter"))
    costcenter = merged_item.get("costcenter")
    
    # Find department URI
    department_uri = null
    department_code = null
    if costcenter:
        # Find the costcenter entry
        cc_entry_full_path_code = rail.find_first_by_attr_and_get_attr(costcenters_group_data, "code", costcenter, "fullpath_code")
        if cc_entry_full_path_code:
            # Split the fullpath_code and get the 4th element
            path_parts = cc_entry_full_path_code.split('/')
            if len(path_parts) > 3:
                fourth_code = path_parts[3]
                # Find the URI for the 4th level code
                department_uri = rail.find_first_by_attr_and_get_attr(
                    departments_group_data, "code", fourth_code, "uri"
                ) if fourth_code else null
                department_code = fourth_code
    
    # Then add the URI lookups using the merged data
    merged_item.update({
        "calculated_orgstructure_uri": get_calculated_group_uri(orgstsructure_group_data, merged_item.get("orgstructure"), 
            config.defaults_mapper_data["dummy_org_structure_code"]),
        "calculated_costcenter_uri": get_calculated_group_uri(costcenters_group_data, merged_item.get("costcenter"), 
            config.defaults_mapper_data["dummy_cost_center_code"]),
        "calculated_department_uri": department_uri if department_uri else rail.find_first_by_attr_and_get_attr(departments_group_data, "code",
                config.defaults_mapper_data["dummy_department_code"], "uri"),
        "calculated_department": department_code if department_code else config.defaults_mapper_data["dummy_department_code"],
        "replicon_org_structures": get_cached_result("get_location_group_as_orgstructure")
    })
    
    return merged_item

def get_calculated_group_uri(group_data, group_code, default_mapper_code):
    if group_code:
        uri = rail.find_first_by_attr_and_get_attr(
            group_data, "code", group_code, "uri")
        if uri:
            return uri
        return rail.find_first_by_attr_and_get_attr(group_data, "code",
                default_mapper_code, "uri")
    return rail.find_first_by_attr_and_get_attr(group_data, "code",
                default_mapper_code, "uri")



def get_login_status(legal_unit, org_structure_code, employee_type, cost_center, login_status_mapper_data):
    """
    Determine login status based on legal unit, company code, employee type and cost center
    Uses list-based mapper format with inclusion/exclusion logic
    
    Logic:
    - If cost_center list is not empty, it's an inclusion list - user's cost_center must be IN the list
    - If cost_center list is empty, check cost_center_exclude - user's cost_center must NOT be in the exclude list
    """
    for mapping in login_status_mapper_data:
        # Check if legal_number, org_structure_code, and employee_type match
        if (mapping.get('legal_number') == legal_unit and 
            mapping.get('org_structure_code') == org_structure_code and 
            mapping.get('employee_type') == employee_type):
            
            # Get the cost center lists
            cost_center_list = mapping.get('cost_center', [])
            cost_center_exclude_list = mapping.get('cost_center_exclude', [])
            
            # Check cost center logic
            if cost_center_list:  # If cost_center list is not empty, it's an inclusion list
                if cost_center in cost_center_list:
                    return mapping.get('status')
            else:  # If cost_center list is empty, check exclusion list
                if cost_center not in cost_center_exclude_list:
                    return mapping.get('status')
    
    return null  # Default status if no match found

def get_employee_type(org_structure_code, work_relationship, employment_type, employment_subtype, 
                     employee_type_mapper_data, manager_flag):
    """
    Determine employee type based on org structure, work relationship, employment type and subtype
    Uses ONLY standard mapping rules - exception preservation is handled in request_payload.py
    """
    
    # Use standard mappings for both ADD and UPDATE scenarios
    if employee_type_mapper_data and 'standard' in employee_type_mapper_data:
        mapper_data = employee_type_mapper_data['standard']
    else:
        mapper_data = employee_type_mapper_data
    
    # Process mappings in order (first match wins)
    for mapping in mapper_data:
        if mapping.get('org_structure_code') == org_structure_code:
            # Check work relationship match
            if mapping.get('work_relationship') != work_relationship:
                continue
            
            # Check employment type
            employment_types_include = mapping.get('employment_type_include', [])
            employment_types_exclude = mapping.get('employment_type_exclude', [])
            
            # If includes is not empty, check if type is in includes
            if employment_types_include:
                if employment_type not in employment_types_include:
                    continue
            # If includes is empty, check if type is NOT in excludes
            elif employment_types_exclude:
                if employment_type in employment_types_exclude:
                    continue
            
            # Check employment subtype (same logic)
            subtype_include = mapping.get('employment_subtype_include', [])
            subtype_exclude = mapping.get('employment_subtype_exclude', [])
            
            # If includes is not empty, check if subtype is in includes
            if subtype_include:
                if employment_subtype not in subtype_include:
                    continue
            # If includes is empty, check if subtype is NOT in excludes
            elif subtype_exclude:
                if employment_subtype in subtype_exclude:
                    continue

            # Check manager flag - empty list means allow all values
            manager_flags = mapping.get('manager_flag', [])
            if manager_flags and manager_flag not in manager_flags:
                continue
            
            # All conditions matched
            return mapping.get('employee_type', '')
    
    return null  # Default

def get_permissions(org_structure_code, employee_type, permissions_mapper_data):
    """
    Get permissions based on org structure code and employee type
    Uses list-based mapper format
    First checks for exact match, then falls back to 'All' employee type
    """
    # First try exact match
    for mapping in permissions_mapper_data:
        if (mapping.get('org_structure_code') == org_structure_code and
            mapping.get('employee_type') == employee_type):
            permissions = mapping.get('permission')
            return permissions

    # Fallback to 'All' employee type if no exact match found
    for mapping in permissions_mapper_data:
        if (mapping.get('org_structure_code') == org_structure_code and
            mapping.get('employee_type') == 'All'):
            permissions = mapping.get('permission')
            return permissions

    return []  # Default empty permissions

def get_activities(org_structure_code, employee_type, activities_mapper_data):
    """
    Get activities based on org structure code and employee type
    Returns activities as a list
    """
    for mapping in activities_mapper_data:
        if (mapping.get('org_structure_code') == org_structure_code and 
            mapping.get('employee_type') == employee_type):
            activities = mapping.get('activities')
            return activities
    return []

def get_time_zone(country, time_zone_mapper_data):
    """
    Get time zone based on country
    """
    if country in time_zone_mapper_data:
        return time_zone_mapper_data[country]
    return ''


def get_display_name(first_name, last_name, defaults_mapper_data=null):
    """
    Generate display name from first and last name with defaults
    """
    if defaults_mapper_data:
        fname = first_name or defaults_mapper_data.get('first_name', 'unknown')
        lname = last_name or defaults_mapper_data.get('last_name', 'unknown')
    else:
        fname = first_name or 'unknown'
        lname = last_name or 'unknown'
    return f"{fname} {lname}"


def apply_tsystems_business_logic(user_data, config):
    """
    Apply T-Systems specific business logic to determine employee type, login status, etc.
    This function must be called first to derive employee type before other mappers
    """
    result = {}
    
    # STEP 1: Derive employee type first (required for other mappings)
    employee_type = get_employee_type(
        user_data.get('orgstructure'),
        user_data.get('type_of_work_relationship'),
        user_data.get('type_of_employment'),
        user_data.get('sub_type_of_employment'),
        config.employee_type_mapper_data,  # This should be 5th
        user_data.get('manager_flag'),  # This should be 6th
    )
    result['calculated_employee_type'] = employee_type
    result['calculated_employee_type_uri'] = rail.find_first_by_attr_and_get_attr(
        cached_load_all_records(get_cached_result("get_all_employeetypes")), "displayText", employee_type, "uri")
    
    # STEP 2: Derive login status using the calculated employee type
    login_status = get_login_status(
        user_data.get('legalunit'),
        user_data.get('orgstructure'),
        employee_type,
        user_data.get('costcenter'),
        config.login_status_mapper_data
    )
    result['calculated_login_status'] = login_status
    
    # STEP 3: Get permissions
    permissions = get_permissions(
        user_data.get('orgstructure'),
        employee_type,
        config.permissions_mapper_data
    )
    result['calculated_permissions'] = {
        permission: get_cached_result("get_required_permission_sets")[permission] for permission in permissions if permission in get_cached_result("get_required_permission_sets").keys()
    }
    
    # STEP 4: Get activities
    activities = get_activities(
        user_data.get('orgstructure'),
        employee_type,
        config.activities_mapper_data
    )
    result['calculated_activities'] = {
        activity: get_cached_result("get_required_activities")[activity] for activity in activities if activity in get_cached_result("get_required_activities").keys()
    }
    
    # STEP 5: Get timesheet template
    timesheet_template = get_timesheet_template(
        user_data.get('orgstructure'),
        user_data.get('type_of_work_relationship'),
        user_data.get('type_of_employment'),
        user_data.get('sub_type_of_employment'),
        employee_type,
        user_data.get('manager_flag'),
        config.timesheet_template_mapper_data
    )
    result['calculated_timesheet_template'] = timesheet_template
    result['calculated_timesheet_template_uri'] = (get_cached_result("get_required_timesheet_templates")[timesheet_template]
        if timesheet_template in get_cached_result("get_required_timesheet_templates").keys() else null)
    
    # STEP 6: Get time off types
    time_off_types = get_time_off_types(
        user_data.get('orgstructure'),
        config.time_off_type_mapper_data
    )
    result['calculated_time_off_types'] = {
        timeofftype: get_cached_result("get_required_time_off_types")[timeofftype]
        for timeofftype in time_off_types if timeofftype in get_cached_result("get_required_time_off_types").keys()
    }
    
    # STEP 7: Get time zone
    time_zone = get_time_zone(
        user_data.get('country_of_employment'),
        config.time_zone_mapper_data
    )
    result['calculated_time_zone'] = time_zone
    result['calculated_time_zone_uri'] = rail.find_first_by_attr_and_get_attr(get_cached_result("get_all_timezones"),
        "displayText", time_zone, "uri") 
    
    # STEP 10: Get display name
    display_name = get_display_name(
        user_data.get('firstname'),
        user_data.get('lastname')
    )
    result['calculated_display_name'] = display_name
    
    return result

def get_timesheet_template(org_structure_code, work_relationship, employment_type, 
                          employment_subtype, employee_type, manager_flag, 
                          timesheet_template_mapper_data):
    """
    Get timesheet template based on org structure, work relationship, employment type, and employee type
    Uses ONLY standard mapping rules - exception preservation is handled in request_payload.py
    Returns null for cases where no template should be assigned (empty string in mapper)
    """
    
    # Use standard mappings for both ADD and UPDATE scenarios
    if timesheet_template_mapper_data and 'standard' in timesheet_template_mapper_data:
        mapper_data = timesheet_template_mapper_data['standard']
    else:
        mapper_data = timesheet_template_mapper_data
    
    # Process mappings in order (first match wins)
    for mapping in mapper_data:
        if (mapping.get('org_structure_code') == org_structure_code and
            mapping.get('employee_type') == employee_type):
            
            # Check work relationship if specified
            if mapping.get('work_relationship') and mapping.get('work_relationship') != work_relationship:
                continue
            
            # Check employment type
            employment_types_include = mapping.get('employment_type_include', [])
            employment_types_exclude = mapping.get('employment_type_exclude', [])
            
            # If includes is not empty, check if type is in includes
            if employment_types_include:
                if employment_type not in employment_types_include:
                    continue
            # If includes is empty, check if type is NOT in excludes
            elif employment_types_exclude:
                if employment_type in employment_types_exclude:
                    continue
            
            # Check employment subtype (same logic)
            subtype_include = mapping.get('employment_subtype_include', [])
            subtype_exclude = mapping.get('employment_subtype_exclude', [])
            
            # If includes is not empty, check if subtype is in includes
            if subtype_include:
                if employment_subtype not in subtype_include:
                    continue
            # If includes is empty, check if subtype is NOT in excludes
            elif subtype_exclude:
                if employment_subtype in subtype_exclude:
                    continue
            
            # Check manager flag - empty list means allow all values
            manager_flags = mapping.get('manager_flag', [])
            if manager_flags and manager_flag not in manager_flags:
                continue
            
            # All conditions matched
            template = mapping.get('timesheet_template')
            
            # Handle empty string (no template) cases
            if template == "":
                return null
                
            return template
    
    return null  # No match found

def get_time_off_types(org_structure_code, time_off_type_mapper_data):
    """
    Get time off types based on org structure code
    """
    time_off_types = []
    
    for mapping in time_off_type_mapper_data:
        if mapping.get('org_structure_code') == org_structure_code:
            time_off_type = mapping.get('time_off_type')
            if time_off_type:
                time_off_types.append(time_off_type)
    
    return time_off_types if time_off_types else ['Absences', 'Vacations', 'Long-term reduction', 'Bank Holidays']


def logging_details(time_zone, STANDARD_EMAIL_DATE_FORMAT, YMD_DATE_FORMAT):
    """
    Log import start details
    """
    today = pendulum.now(time_zone)
    return {
        "current_date": today.strftime(YMD_DATE_FORMAT),
        "process_start_time": today.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "log_filename": f'Logs_User_sync_{today.strftime("%Y%m%dT%H%M%S")}.csv',
    }

def get_invalid_user_log_details(user_data):
    """
    Get details for invalid user logs
    """
    missing_fields = []
    if not user_data.get('email'):
        missing_fields.append('Email')
    if not user_data.get('employeeid'):
        missing_fields.append('Employee ID')
    if not user_data.get('startdate'):
        missing_fields.append('Start Date')
    
    return f"Missing required fields: {', '.join(missing_fields)}"

def get_error_message():
    """
    Get current error message from task context
    """
    try:
        # This would typically get the error from Airflow context
        return rail.get_current_context().get('exception', 'Unknown error occurred')
    except:
        return 'Unknown error occurred'

def do_format_logs(dag_run):
    # Get logs from dag_run.conf
    user_logs = dag_run.conf.get('user_logs', []) + [dag_run.conf.get('create_log')]
    supervisor_assignment_log = dag_run.conf.get('supervisor_assignment_log')
    
    # Process user logs first
    user_log_artifacts = []
    if user_logs:
        if isinstance(user_logs, list):
            user_log_artifacts.extend(user_logs)
        else:
            user_log_artifacts.append(user_logs)
    
    # Load all user log records
    user_log_records = []
    for log in user_log_artifacts:
        if log:  # Check if log exists
            each_log_records = cached_load_all_records(log)
            if each_log_records:
                user_log_records.extend(each_log_records)
    
    # Convert user logs to final format with runid
    formatted_user_records = {}
    blank_user_records = []
    
    for log in user_log_records:
        employee_id = log['properties'].get('employeeid', '')
        if employee_id:
            formatted_user_records[employee_id] = {
                **log['properties'],
                "runid": log['ecid']
            }
        else:
            # Keep logs with blank employee IDs separate
            blank_user_records.append({
                **log['properties'],
                "runid": log['ecid']
            })
    
    # Process supervisor logs separately
    supervisor_log_records = []
    if supervisor_assignment_log:
        supervisor_log_records = cached_load_all_records(supervisor_assignment_log) or []
    
    # Convert supervisor logs to dictionary by employeeid (only for non-blank IDs)
    supervisor_records_by_emp = {}
    for log in supervisor_log_records:
        employee_id = log['properties'].get('employeeid', '')
        if employee_id:  # Only add to dict if employee_id is not blank
            supervisor_records_by_emp[employee_id] = {
                **log['properties'],
                "runid": log['ecid']
            }
    
    # Merge logs
    final_log_records = []
    # Priority: Error (1) > Exception (2) > Success (3) > Skipped (4)
    # Lower number = higher priority (will override other statuses)
    status_priority = {"Error": 1, "Exception": 2, "Success": 3, "Skipped": 4}
    
    # Process all employees
    all_employee_ids = set(formatted_user_records.keys()) | set(supervisor_records_by_emp.keys())
    
    for emp_id in all_employee_ids:
        user_log = formatted_user_records.get(emp_id)
        supervisor_log = supervisor_records_by_emp.get(emp_id)
        
        if user_log and supervisor_log:
            # Merge supervisor details into user log
            merged_log = user_log.copy()
            
            # Determine final status based on priority
            user_priority = status_priority.get(user_log.get('status'), 5)
            sup_priority = status_priority.get(supervisor_log.get('status'), 5)
            if sup_priority < user_priority:
                merged_log['status'] = supervisor_log['status']
            
            # Append supervisor details to user details
            merged_log['details'] = user_log.get('details', '') + ' | ' + supervisor_log.get('details', '')
            final_log_records.append(merged_log)
        elif user_log:
            final_log_records.append(user_log)
        elif supervisor_log:
            final_log_records.append(supervisor_log)
    
    # Add all logs with blank employee IDs at the end
    final_log_records.extend(blank_user_records)

    rail.set_result(key="get_logged_success", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_logged_errors", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_logged_exceptions", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))
    rail.set_result(key="get_logged_skipped", val=len(list(filter(lambda item: item['status']=="Skipped", final_log_records))))

    return final_log_records

def load_user_details_from_artifacts(dag_run):
    user_data_artifact = get_cached_result("filter_users_data_from_source") or []
    # Load and flatten all records, then extract properties
    return cached_load_all_records(user_data_artifact) if user_data_artifact else []


def get_email_log_details(log_file_path, dag_run, time_zone, STANDARD_EMAIL_DATE_FORMAT):
    current_time = pendulum.now(time_zone)
    start_time_str = dag_run.conf['process_start_time']
    return {
        "job_start_time": start_time_str,
        "job_end_time": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).total_seconds() / 60, 1),
        "log_file_name": dag_run.conf['log_filename'],
        "log_file_path": log_file_path,
        "total_record_count": dag_run.conf['total_record_count']
    }

