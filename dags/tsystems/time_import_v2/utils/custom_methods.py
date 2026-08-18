"""
T-Systems Time Import Custom Methods and Utility Functions
"""
from datetime import datetime
from pendulum import now
from dateutil.relativedelta import relativedelta
from ast import literal_eval
from typing import Dict, List, Any, Optional, Set
import rail
from tsystems.time_import_v2 import config

# Constants
MANDATORY_FIELDS = ['Reported by', 'Employee ID', 'Entry Date']
DIST_MANDATORY_FIELDS = ['Reported by', 'Employee ID', 'Entry Date', 'Project ID', 'Task Name', 'Activity', 'Hours']
INOUT_DIST_MANDATORY_FIELDS = ['Reported by', 'Employee ID', 'Entry Date']
INOUT_DIST_WITH_OEF_MANDATORY_FIELDS = ['Reported by', 'Employee ID', 'Entry Date']
FEED_ENTRYDATE_DATE_FORMAT = "%d/%m/%Y"
FEED_ENTRYTIME_TIME_FORMAT = "%H:%M"
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

null = None

def get_username_for_employee_id(employee_id: str) -> str:
    """
    Look up a resolved username for an employee ID from the master DAG's bulk
    employee lookup (get_all_employees_user_details). Used by log sites in
    master.py that run before any per-employee Replicon lookup would otherwise occur.

    Returns blank if the employee_id is empty, unresolved, or the bulk lookup
    was skipped (e.g. no employee IDs were present in the file at all).
    """
    if not employee_id:
        return ''
    username_map = rail.result('get_all_employees_user_details')
    return (username_map or {}).get(employee_id, '')

def is_reported_by_user_eligible_for_time_import(oef_name):
    """
    Check if the reported by user is eligible for time import.
    
    Returns:
        bool: True if the user is eligible, False otherwise
    """
    eligible_for_time_import_oef_value = rail.find_first_by_attr_and_get_attr(
        rail.result("get_reported_by_user_details")["userDetails"]["extensionFieldValues"],
        "definition.displayText", oef_name, "tag.displayText")
    return True if eligible_for_time_import_oef_value == "Yes" else False

def validate_mandatory_fields(csv_record: Dict[str, Any]) -> List[str]:
    """
    Validate that mandatory fields are present and not empty.
    
    Checks that employee_id and entry_date fields exist and contain valid data.
    These are the core required fields for all time import records.
    
    Args:
        csv_record: Dictionary containing CSV record data with field mappings
        
    Returns:
        List[str]: List of missing field error messages, empty if all fields valid
    """
    missing_fields = []
    for field in MANDATORY_FIELDS:
        if not csv_record.get(field) or str(csv_record[field]) == '':
            missing_fields.append(f"Blank {field} found in input file")
    
    return missing_fields

def validate_ts_based_mandatory_fields(record: Dict[str, Any], ts_type: str) -> str:
    """
    Validate fields based on the user's timesheet template type.
    
    This is Phase 2 validation that handles template-specific requirements:
    - Time Distribution only: Project ID, Task Name, Activity, Hours (all required); In/Out and WorkType N/A
    - In/Out plus Time Distribution: All fields optional except Employee ID, Entry Date; WorkType N/A
    - In/Out with OEF plus Time Distribution: All fields optional except Employee ID, Entry Date

    Args:
        record: Dictionary containing time entry record data
        ts_type: Timesheet template type determining validation rules

    Returns:
        str: Error message if record should not be processed,
             empty string if record can be processed
    """
    # Check if record already has a validation error from basic validation
    if 'validation_error' in record:
        # These are critical errors that already blocked the record
        return record['validation_error']
    
    # Track daily hours and time slots for validation within the template context
    validation_errors = []
    
    if ts_type == config.timesheet_dist:
        # Time Distribution only - strict validation
        
        # Check mandatory fields
        mandatory_fields = {
            'project_id': 'Project ID',
            'task_name': 'Task Name', 
            'activity': 'Activity',
            'hours': 'Hours'
        }
        
        for field, column in mandatory_fields.items():
            if not record.get(field) or str(record[field]) == '':
                validation_errors.append(f'Blank {column} found in input file')
        
        # Validate hours if provided
        hours_str = record.get('hours', '')
        if hours_str:
            try:
                hours = float(hours_str)
                if hours < 0:
                    validation_errors.append('Hours format error')
                elif hours == 0:
                    validation_errors.append('Hours must be greater than 0')
            except (ValueError, TypeError):
                validation_errors.append('Hours format error')
        
        # Check for N/A fields that shouldn't be present
        if record.get('in_time', '') or record.get('out_time', ''):
            # Log warning but don't block - clock times are N/A for distribution
            pass
            
    elif ts_type == config.timesheet_inout_dist:
        # In/Out plus Time Distribution - flexible validation
        # Note: Clock in/out validation moved to validate_inout_and_worktype in process_each_in_out
        
        # Check hours if provided
        hours_str = record.get('hours', '')
        if hours_str:
            try:
                hours = float(hours_str)
                if hours < 0:
                    validation_errors.append('Hours format error')
            except (ValueError, TypeError):
                validation_errors.append('Hours format error')
            
    elif ts_type == config.timesheet_inout_dist_with_oef:
        # In/Out with OEF plus Time Distribution - most flexible
        # Note: Clock in/out and WorkType validation moved to validate_inout_and_worktype in process_each_in_out
        
        # Check hours if provided
        hours_str = record.get('hours', '')
        if hours_str:
            try:
                hours = float(hours_str)
                if hours < 0:
                    validation_errors.append('Hours format error')
            except (ValueError, TypeError):
                validation_errors.append('Hours format error')
        
        # Note: WorkType validation is done separately in validate_worktype_for_oef_timesheet
    
    # Return the first validation error if any
    if validation_errors:
        return validation_errors[0]
    
    return ''  # No validation errors, can process

def validate_and_separate_records(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Validate all records and separate into valid and invalid lists.
    
    Processes all CSV records through mandatory field validation and categorizes
    them based on validation results for further processing.
    
    Args:
        records: List of dictionaries containing CSV record data
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Dictionary containing:
            - 'valid_records': Records that passed validation
            - 'invalid_records': Records that failed validation
    """
    valid_records = []
    invalid_records = []
    
    for record in records:
        validation_errors = validate_mandatory_fields(record)
        if validation_errors:
            invalid_records.append(record)
        else:
            valid_records.append(record)
    
    return {
        'valid_records': valid_records,
        'invalid_records': invalid_records
    }

def get_validation_error_message(record: Dict[str, Any]) -> str:
    """
    Generate a descriptive validation error message for a failed record.
    
    Converts the internal field mappings back to original CSV column names
    for user-friendly error reporting.
    
    Args:
        record: Dictionary containing failed CSV record data with mapped field names
        
    Returns:
        str: Human-readable error message describing validation failures
    """
    # Convert underscored keys back to original format for validation
    original_record = {
        'Reported by': record.get('reported_by', ''),
        'Employee ID': record.get('employee_id', ''),
        'Entry Date': record.get('entry_date', '')
    }
    errors = validate_mandatory_fields(original_record)
    if errors:
        return "; ".join(errors)
    return "No validation errors"


def group_timesheet_uris_by_status(timesheet_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Extract unique timesheet URIs split by submission status.

    Args:
        timesheet_data: List of dictionaries containing timesheet status information.

    Returns:
        Dict with two keys:
          'submitted' — URIs for timesheets that need reopening (status != 'Not Submitted')
          'all'       — URIs for every timesheet (submitted first, then not-submitted)
    """
    submitted = set()
    not_submitted = set()

    for timesheet in timesheet_data:
        uri = timesheet.get('timesheet_uri')
        if uri:
            if timesheet.get('timesheet_status') != 'Not Submitted':
                submitted.add(uri)
            else:
                not_submitted.add(uri)

    all_uris = submitted | not_submitted
    return {
        "submitted": [{'ts_uri': ts} for ts in submitted],
        "all": [{'ts_uri': ts} for ts in all_uris]
    }

def is_timesheet_type_not_distribution_only(dag_run) -> bool:
    """
    Check if the user's timesheet type is not "Time Distribution only".
    
    Args:
        dag_run: Airflow DAG run object containing entry data in conf
        
    Returns:
        bool: True if timesheet type is not "Time Distribution only", False otherwise
    """
    ts_type = dag_run.conf.get('user_ts_type', '')
    return ts_type != config.timesheet_dist

def check_time_format_error(dag_run) -> str:
    """
    Check if there are any time format errors in the in/out times.
    
    Args:
        dag_run: Airflow DAG run object containing entry data in conf
        
    Returns:
        str: Error message if time format validation fails, empty string if valid
    """
    in_time = dag_run.conf.get('in_time', '')
    out_time = dag_run.conf.get('out_time', '')
    
    # Validate in/out times - if one is present, both must be present
    if (in_time and not out_time) or (out_time and not in_time):
        return 'Clock In time/Clock Out time is missing. Do not import time if any of the Clock In/Out time is missing'

    # If both are missing, skip silently (no attendance data for this record)
    if not in_time and not out_time:
        return ''

    # Validate time format
    try:
        datetime.strptime(in_time, config.time_format)
    except ValueError:
        return f'Invalid In time hours format. Expected format is {config.time_format}'
    
    try:
        datetime.strptime(out_time, config.time_format)
    except ValueError:
        return f'Invalid Out time hours format. Expected format is {config.time_format}'
    
    return ''  # No error


def check_worktype_error(dag_run) -> str:
    """
    Check if WorkType is valid for OEF timesheet templates.
    
    Args:
        dag_run: Airflow DAG run object containing entry data and OEF configurations
        
    Returns:
        str: Error message if worktype validation fails, empty string if valid
    """
    ts_type = dag_run.conf.get('user_ts_type', '')
    
    # Only validate WorkType for In/Out with OEF timesheet
    if ts_type != config.timesheet_inout_dist_with_oef:
        return ''  # No validation needed for other timesheet types
    
    work_type = dag_run.conf.get('work_type', '')
    
    if not work_type:
        return ''  # Empty worktype is allowed
    
    # Check if work type exists in any of the OEF tag lists
    worktype_oef_tags = dag_run.conf.get('worktype_oef_tags', [])
    worktype_tarif_oef_tags = dag_run.conf.get('worktype_tarif_oef_tags', [])
    worktype_tariffrei_oef_tags = dag_run.conf.get('worktype_tariffrei_oef_tags', [])
    
    worktype_found = False
    
    if rail.find_first_by_attr_and_get_attr(worktype_oef_tags, 'name', work_type, 'uri', None):
        worktype_found = True
    elif rail.find_first_by_attr_and_get_attr(worktype_tarif_oef_tags, 'name', work_type, 'uri', None):
        worktype_found = True
    elif rail.find_first_by_attr_and_get_attr(worktype_tariffrei_oef_tags, 'name', work_type, 'uri', None):
        worktype_found = True
    
    if not worktype_found:
        return f'Work Type "{work_type}" is not available'
    
    return ''  # No error


def get_oef_and_tags_details(dag_run) -> Dict[str, str]:
    """
    Determine appropriate Object Extension Field (OEF) configuration for time entry.

    Uses the employee's timesheet template (user_ts_template) to identify which
    OEF is assigned, then looks up the work type value within that specific OEF only.
    This avoids returning the wrong OEF when the same work type value exists in
    multiple OEFs.

    Args:
        dag_run: Airflow DAG run object containing entry data and OEF configurations

    Returns:
        Dict[str, str]: Dictionary containing 'oef_uri' and 'oef_tag_uri' keys,
                        or empty dictionary if no matching worktype found
    """
    worktype = dag_run.conf['work_type']
    user_ts_template = dag_run.conf.get('user_ts_template', '')
    assigned_oef = config.template_worktype_oef_mapper.get(user_ts_template)

    result = {}

    oef_conf_keys = {
        config.worktype             : ('worktype_oef', 'worktype_oef_tags'),
        config.worktype_tarif       : ('worktype_tarif_oef', 'worktype_tarif_oef_tags'),
        config.worktype_tariffrei   : ('worktype_tariffrei_oef', 'worktype_tariffrei_oef_tags'),
    }
    oef_key, tags_key = oef_conf_keys.get(assigned_oef, (None, None))
    if oef_key:
        tag_uri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf[tags_key], 'name', worktype, 'uri', False)
        if tag_uri:
            result['oef_uri'] = dag_run.conf[oef_key]
            result['oef_tag_uri'] = tag_uri

    return result


def load_records(log_artifact):
    """
    Load all records from a log artifact.
    
    Args:
        log_artifact: Log artifact containing record data
        
    Returns:
        List of all records from the log artifact
    """
    return rail.load_all_records(log_artifact)

def get_status(item: Dict[str, Any], logstatus: str) -> bool:
    """
    Check if an item's status matches the specified log status.
    
    Args:
        item: Dictionary containing item with status field
        logstatus: Status string to match against (case-insensitive)
        
    Returns:
        bool: True if item status matches logstatus (case-insensitive)
    """
    return item['status'].lower() == logstatus


def do_format_logs(dag_run) -> List[Dict[str, Any]]:
    """
    Process and format logs from all processing activities into final report format.
    
    Consolidates logs from record validation and time entry processing, removes
    duplicates, determines final status for each unique record combination,
    and prepares data for CSV report generation.
    
    Args:
        dag_run: Airflow DAG run object containing log artifact references
        
    Returns:
        List[Dict[str, Any]]: Formatted log records ready for CSV generation,
                              with status counts set as DAG run results
    """
    log_artifacts = []
    log_records = []

    timeentrylogs = dag_run.conf['timeentrylogs']
    otherlogs = dag_run.conf['otherlogs']

    if timeentrylogs:
        if isinstance(timeentrylogs, list):
            log_artifacts.extend(timeentrylogs)
        elif isinstance(timeentrylogs, str) and timeentrylogs[0] == '[':
            timeentrylogs = literal_eval(timeentrylogs)
            log_artifacts.extend(timeentrylogs)
        else:
            log_artifacts.append(timeentrylogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    def get_log_status(entry_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], entry_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"

    final_log_records = []
    
    # Group logs by row_number
    logs_by_row = {}
    for log_record in log_records:
        row_num = log_record['properties'].get('row_number', '')
        if row_num:
            if row_num not in logs_by_row:
                logs_by_row[row_num] = []
            logs_by_row[row_num].append(log_record)
    
    # Process each row's logs
    for row_num, entry_logs in logs_by_row.items():
        if entry_logs:
            first = entry_logs[0]
            
            # Collect unique detail messages
            details = []
            for log in entry_logs:
                detail = log['properties'].get('details', '')
                if detail and detail not in details:
                    details.append(detail)
            
            final_log_records.append({
                'row_number': first['properties']['row_number'],
                'employee_id': first['properties']['employee_id'],
                'user_name': first['properties'].get('user_name') or '',
                'entry_date': first['properties']['entry_date'],
                'project_id': first['properties']['project_id'],
                'task_name': first['properties']['task_name'],
                'activity': first['properties']['activity'],
                'action': first['properties']['action'],
                'status': get_log_status(entry_logs),
                'details': '; '.join(details),
                'ecid': first['ecid'],
            })

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: get_status(x, 'skipped'), final_log_records ))))

    return  final_log_records

def check_time_overlap(record1: Dict[str, Any], record2: Dict[str, Any]) -> bool:
    """
    Check if two time records overlap.
    
    Args:
        record1: First record with in_time and out_time
        record2: Second record with in_time and out_time
        
    Returns:
        bool: True if the time ranges overlap, False otherwise
    """
    try:
        # Parse times for both records
        in1 = datetime.strptime(record1['in_time'], config.time_format).time()
        out1 = datetime.strptime(record1['out_time'], config.time_format).time()
        in2 = datetime.strptime(record2['in_time'], config.time_format).time()
        out2 = datetime.strptime(record2['out_time'], config.time_format).time()
        
        # Check for overlap
        # Two time ranges overlap if one starts before the other ends
        return not (out1 <= in2 or out2 <= in1)
    except (ValueError, KeyError):
        # If parsing fails, consider them non-overlapping
        return False

def detect_overlapping_times(records: List[Dict[str, Any]]) -> Set[str]:
    """
    Detect overlapping time entries and return row_numbers that should be marked as overlapping.
    For multiple overlapping entries on same date, first one is processed, rest are marked.
    
    Args:
        records: List of validated records containing time entries
        
    Returns:
        Set[str]: Set of row_numbers that have overlapping times
    """
    overlapping_rows = set()
    
    # Group records by employee_id and entry_date
    grouped = {}
    for record in records:
        if record.get('in_time') and record.get('out_time'):
            key = f"{record['employee_id']}_{record['entry_date']}"
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(record)
    
    # Check each group for overlaps
    for key, group_records in grouped.items():
        if len(group_records) > 1:
            # Sort by row_number to ensure consistent order
            sorted_records = sorted(group_records, key=lambda x: int(x.get('row_number', 0)))
            
            # Check each pair for overlaps
            for i in range(len(sorted_records)):
                if sorted_records[i]['row_number'] in overlapping_rows:
                    continue
                for j in range(i + 1, len(sorted_records)):
                    if check_time_overlap(sorted_records[i], sorted_records[j]):
                        # Mark the later record as overlapping
                        overlapping_rows.add(sorted_records[j]['row_number'])
    
    return overlapping_rows

def check_cumulative_hours(records: List[Dict[str, Any]]) -> Set[str]:
    """
    Check cumulative hours per employee per day and mark records that exceed 24 hour limit.
    
    Args:
        records: List of validated records containing time entries
        
    Returns:
        Set[str]: Set of row_numbers that exceed the daily 24 hour limit
    """
    exceeding_rows = set()
    
    # Group records by employee_id and entry_date
    daily_hours = {}
    for record in records:
        hours_str = record.get('hours', '')
        if hours_str:
            try:
                hours = float(hours_str)
                if hours > 0:  # Only count positive hours
                    key = f"{record['employee_id']}_{record['entry_date']}"
                    if key not in daily_hours:
                        daily_hours[key] = []
                    daily_hours[key].append({
                        'row_number': record.get('row_number', ''),
                        'hours': hours
                    })
            except (ValueError, TypeError):
                # Skip invalid hours
                pass
    
    # Check cumulative hours for each employee-date combination
    for key, hour_records in daily_hours.items():
        # Sort by row_number to process in order
        sorted_hour_records = sorted(hour_records, key=lambda x: int(x.get('row_number', 0)))
        
        cumulative_hours = 0
        for hour_record in sorted_hour_records:
            cumulative_hours += hour_record['hours']
            if cumulative_hours > 24:
                # Mark this record and any subsequent ones as exceeding limit
                exceeding_rows.add(hour_record['row_number'])
    
    return exceeding_rows

def validate_records_format():
    """
    Performs comprehensive validation on time import records.
    
    Validates all requirements including:
    - Employee ID presence 
    - Entry date format (DD/MM/YYYY)
    - Entry date vs user start date comparison
    - M-1 and M+1 date restrictions
    - Overlapping time entries
    - Cumulative hours exceeding 24 hour limit
    
    Template-specific validations are handled later in process_each_entry.
    
    Returns:
        dict: {
            'valid_records': [...],  # Records passing all validation
            'invalid_records': [...],  # Records with any validation errors
            'unique_entry_dates': [...] # Unique valid entry dates for in/out processing
        }
    """
    records = rail.load_all_records(rail.result('get_all_records_for_user'))
    
    # Get user employment start date
    user_details = rail.result('get_user_details')
    startdate = user_details['userDetails']['employmentDateRange']['startDate'] if user_details['userDetails'].get('employmentDateRange') else None
    user_startdate = convert_date_dict_to_datetime(startdate) if startdate else datetime.max
    
    # First pass: Detect overlapping times and cumulative hours
    overlapping_rows = detect_overlapping_times(records)
    exceeding_rows = check_cumulative_hours(records)
    
    valid_records = []
    invalid_records = []
    unique_dates = []
    
    # Calculate M-1 and M+1 boundaries
    current_date = now()
    first_day_previous_month = current_date.date() + relativedelta(months=-1, day=1)
    next_30th_day = current_date.date() + relativedelta(days=30)
    
    # Second pass: Validate each record
    for record in records:
        validation_error = None
        
        # Check if overlapping
        if record.get('row_number', '') in overlapping_rows:
            validation_error = 'Overlapping time entries are not allowed'
        
        # Check if exceeds daily limit
        elif record.get('row_number', '') in exceeding_rows:
            validation_error = 'If more than 24 hours are identified in the file, the hours will not be updated'
        
        # Check if entry_date is present
        elif 'entry_date' not in record or not record['entry_date']:
            validation_error = 'Date format error.'
        else:
            try:
                # Parse and validate date format
                entry_date = datetime.strptime(record['entry_date'], config.entry_dateformat)
                
                # Check if entry date is before user's start date
                if entry_date < user_startdate:
                    validation_error = 'Entry date cannot be before user employment start date'
                
                # Check M-1 (entries not allowed prior to first day of previous month)
                elif entry_date.date() < first_day_previous_month:
                    validation_error = 'Time entries prior to M-1 are not allowed'
                
                # Check M+1 (entries not allowed after 30 days from current date)
                elif entry_date.date() > next_30th_day:
                    validation_error = 'Timesheet does not exist'
                    
            except ValueError:
                # Date parsing failed
                validation_error = 'Date format error'
        
        # Add to appropriate list
        if validation_error:
            record['validation_error'] = validation_error
            invalid_records.append(record)
        else:
            valid_records.append(record)
            unique_dates.append(record)
    
    return {
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'unique_entry_dates': unique_dates
    }

def check_user_assigned_to_project(dag_run):
    """
    Checks if user is assigned to the project team either directly or via group assignment.

    Validates user access to project by checking:
    1. Direct user assignment in the project team
    2. Group-based assignment via Service Center, Department Group, Org Structure (Location), or Employee Type

    Args:
        dag_run: Airflow DAG run object containing user_uri and user_groups

    Returns:
        bool: True if user is assigned to project (directly or via group), False otherwise
    """
    project_details = rail.result('get_all_project_details')
    user_uri = dag_run.conf.get('user_uri', '')
    user_groups = dag_run.conf.get('user_groups') or {}

    if not project_details or 'team' not in project_details:
        return False

    # Mapping: resource_type_uri -> (user_groups_key, resource_nested_key)
    GROUP_TYPE_MAPPING = {
        'urn:replicon:resource-type:location': ('location_uri', 'location'),
        'urn:replicon:resource-type:service-center': ('service_center_uri', 'serviceCenter'),
        'urn:replicon:resource-type:department-group': ('department_uri', 'departmentGroup'),
        'urn:replicon:resource-type:employee-type-group': ('employee_type_uri', 'employeeTypeGroup'),
    }

    for team_member in project_details.get('team', []):
        resource = team_member.get('resource')
        if not resource:
            continue

        resource_type = resource.get('resourceType', {})
        resource_type_uri = resource_type.get('uri', '') if resource_type else ''

        # Direct user assignment
        if resource_type_uri == 'urn:replicon:resource-type:user':
            user = resource.get('user')
            if (user and user.get('uri') == user_uri) or resource.get('uri') == user_uri:
                return True

        # Group-based assignments
        elif resource_type_uri in GROUP_TYPE_MAPPING:
            group_key, nested_key = GROUP_TYPE_MAPPING[resource_type_uri]
            user_group_uri = user_groups.get(group_key)
            if user_group_uri:
                nested_obj = resource.get(nested_key)
                if (nested_obj and nested_obj.get('uri') == user_group_uri) or resource.get('uri') == user_group_uri:
                    return True

    return False

def check_billing_rate_allowed_for_user(dag_run):
    """
    Checks if billing rate is allowed for the user on this project.

    For fixed bid projects, billing rate validation is skipped since fixed bid
    projects don't have billing rates assigned to users. The customer can send
    "Project Rate" or blank in the input file for fixed bid projects.

    Args:
        dag_run: Airflow DAG run object containing user_uri, billing_rate_uri,
                 and entry_data with billing_rate_name

    Returns:
        bool: True if billing rate is allowed for user, False otherwise
    """
    project_details = rail.result('get_all_project_details')
    user_uri = dag_run.conf.get('user_uri', '')
    billing_rate_uri = dag_run.conf.get('billing_rate_uri', '')

    if not dag_run.conf["entry_data"]["billing_rate_name"]:
        return True  # No billing rate specified, so no restriction

    # Check if project_details has required information
    if not project_details:
        return False

    # Get project billing type
    project = project_details.get('project', project_details)
    billing_type = project.get('billingType')
    billing_type_uri = billing_type.get('uri') if billing_type else None

    # For fixed bid projects, only "Project Rate" is allowed
    if billing_type_uri == 'urn:replicon:billing-type:fixed-bid':
        billing_rate_name = dag_run.conf["entry_data"]["billing_rate_name"]
        return billing_rate_name.lower() == 'project rate'

    # For time and material projects, validate billing rate against user's allowed rates
    if 'team' not in project_details:
        return False

    user_groups = dag_run.conf.get('user_groups') or {}

    # Mapping: resource_type_uri -> (user_groups_key, resource_nested_key)
    GROUP_TYPE_MAPPING = {
        'urn:replicon:resource-type:location': ('location_uri', 'location'),
        'urn:replicon:resource-type:service-center': ('service_center_uri', 'serviceCenter'),
        'urn:replicon:resource-type:department-group': ('department_uri', 'departmentGroup'),
        'urn:replicon:resource-type:employee-type-group': ('employee_type_uri', 'employeeTypeGroup'),
    }

    # Check ALL matching team members (direct + groups) and return True if ANY allows the billing rate
    for team_member in project_details.get('team', []):
        resource = team_member.get('resource')
        if not resource:
            continue

        resource_type = resource.get('resourceType', {})
        resource_type_uri = resource_type.get('uri', '') if resource_type else ''
        is_match = False

        # Direct user assignment
        if resource_type_uri == 'urn:replicon:resource-type:user':
            user = resource.get('user')
            if (user and user.get('uri') == user_uri) or resource.get('uri') == user_uri:
                is_match = True

        # Group-based assignments
        elif resource_type_uri in GROUP_TYPE_MAPPING:
            group_key, nested_key = GROUP_TYPE_MAPPING[resource_type_uri]
            user_group_uri = user_groups.get(group_key)
            if user_group_uri:
                nested_obj = resource.get(nested_key)
                if (nested_obj and nested_obj.get('uri') == user_group_uri) or resource.get('uri') == user_group_uri:
                    is_match = True

        # If this team member matches, check if billing rate is allowed
        if is_match:
            for rate in (team_member.get('billingRatesAllowedForBillingTime') or []):
                billing_rate = rate.get('billingRate')
                if billing_rate and billing_rate.get('uri') == billing_rate_uri:
                    return True

    return False

def get_email_details(timezone, log_file_path, dag_run):
    """
    Generate detailed email notification metadata for time import process.
    
    Creates a dictionary containing timestamps, duration, and file paths
    for use in email notifications. Calculates process duration and formats
    timestamps according to standard formats.
    
    Args:
        timezone (str): Timezone identifier for timestamp generation
        log_file_path (str): Path to the generated log file
        dag_run: Airflow DAG run context containing process metadata
        
    Returns:
        dict: Email notification details including:
            - start_time: ISO format timestamp when process started
            - job_end_time: ISO format timestamp when process completed
            - job_duration_minutes: Process duration in minutes
            - log_timestamp: Compact timestamp for log reference
            - email_timestamp: Full ISO timestamp for email headers
            - log_file_name: Name of the generated log file
            - log_filepath: Full path to the log file
            - input_filename: Original input file name
    """
    current_time = now(timezone)
    start_time_str = dag_run.conf['start_time']
    filename = dag_run.conf['input_filename']
    log_filename = dag_run.conf['log_filename']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.isoformat(),
        "job_duration_minutes": (((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).seconds)//60),
        "log_timestamp": current_time.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "log_file_name": log_filename,
        "log_filepath": log_file_path,
        "input_filename": filename,
    }

def convert_date_dict_to_datetime(date_dict: Optional[Dict]) -> Optional[datetime]:
    """
    Convert a date dictionary with day, month, year keys to a datetime object.
    
    Args:
        date_dict: Dictionary with 'day', 'month', 'year' keys or None
        
    Returns:
        datetime object or None if input is None
    """
    if date_dict is None:
        return None
    return datetime(
        year=date_dict['year'],
        month=date_dict['month'],
        day=date_dict['day']
    )


def get_effective_policy_set(item) -> Optional[Dict[str, Any]]:
    """
    Get the effective policySet for a given entry date.
    
    Logic:
    - If effectiveDate is null, this is the initial policy (applicable from beginning)
    - If endDate is null, this is the current/latest policy (no end date)
    - Return the policy where: effectiveDate <= entry_date <= endDate
    
    Args:
        timesheet_schedule: List of policy objects with effectiveDate, endDate, policySet
        entry_date: The date to find the applicable policy for
        
    Returns:
        The policySet object if found, None otherwise
    """
    timesheet_schedule = rail.result('get_user_details')['timesheetTemplateSchedule']
    entry_date = datetime.strptime(item['entry_date'], config.entry_dateformat)
    for policy in timesheet_schedule:
        effective_date = convert_date_dict_to_datetime(policy.get('effectiveDate'))
        end_date = convert_date_dict_to_datetime(policy.get('endDate'))
        
        # Initial policy (no effectiveDate) with an endDate
        if effective_date is None and end_date is not None:
            if entry_date <= end_date:
                return policy.get('policySet')
        
        # Initial policy (no dates) - applies to everything
        elif effective_date is None and end_date is None:
            return policy.get('policySet')
        
        # Policy with effectiveDate but no endDate (current/latest)
        elif effective_date is not None and end_date is None:
            if entry_date >= effective_date:
                return policy.get('policySet')
        
        # Policy with both dates
        elif effective_date is not None and end_date is not None:
            if effective_date <= entry_date <= end_date:
                return policy.get('policySet')
    
    return None

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def is_submit_reopen_timesheets_failed():
    if get_task_state('submit_reopen_timesheets') == 'failed':
        reason  = rail.result('submit_reopen_timesheets', key="error").get('response').get('json').get('error').get('reason')
        return True
    return False