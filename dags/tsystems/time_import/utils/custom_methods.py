"""
T-Systems Time Import Custom Methods and Utility Functions
"""
from datetime import datetime, timedelta
from pendulum import now
from ast import literal_eval
from typing import Dict, List, Any, Optional
import rail
from tsystems.time_import import config

# Constants
MANDATORY_FIELDS = ['employee_id', 'entry_date']
DIST_MANDATORY_FIELDS = ['employee_id', 'entry_date', 'project_id', 'task_name', 'activity', 'hours']
INOUT_DIST_MANDATORY_FIELDS = ['employee_id', 'entry_date']
INOUT_DIST_WITH_OEF_MANDATORY_FIELDS = ['employee_id', 'entry_date']
FEED_ENTRYDATE_DATE_FORMAT = "%d/%m/%Y"
FEED_ENTRYTIME_TIME_FORMAT = "%H:%M"
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

null = None



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
        if not csv_record.get(field) or str(csv_record[field]).strip() == '':
            missing_fields.append(f"Blank {field.replace('_', ' ').title()} found in input file")
    
    return missing_fields

def validate_ts_based_mandatory_fields(record: Dict[str, Any], ts_type: str) -> str:
    """
    Validate mandatory fields based on the user's timesheet template type.
    
    Different timesheet templates require different field combinations:
    - Distribution only: project_id, task_name, activity, hours required
    - In/Out + Distribution: employee_id, entry_date required
    - In/Out with OEF + Distribution: employee_id, entry_date required

    Args:
        record: Dictionary containing time entry record data
        ts_type: Timesheet template type determining validation rules

    Returns:
        str: Semicolon-separated string of missing field error messages,
             empty string if all required fields are present
    """
    missing_fields = []
    mandatory_fields = DIST_MANDATORY_FIELDS if ts_type == config.timesheet_dist else INOUT_DIST_MANDATORY_FIELDS if ts_type == config.timesheet_inout_dist else INOUT_DIST_WITH_OEF_MANDATORY_FIELDS
    for field in mandatory_fields:
        if not record.get(field) or str(record[field]).strip() == '':
            missing_fields.append(f"Blank {field.replace('_', ' ').title()} found in input file")
    return "; ".join(missing_fields)

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
        'Employee ID': record.get('employee_id', ''),
        'Entry Date': record.get('entry_date', '')
    }
    errors = validate_mandatory_fields(record)
    if errors:
        return "; ".join(errors)
    return "No validation errors"


def get_submitted_timesheet_uris(timesheet_data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Extract unique timesheet URIs for timesheets that need reopening.
    
    Identifies timesheets that are in submitted, approved, or other non-open states
    that need to be reopened before time entries can be modified.
    
    Args:
        timesheet_data: List of dictionaries containing timesheet status information
        
    Returns:
        List[Dict[str, str]]: List of dictionaries with 'ts_uri' key containing
                              unique timesheet URIs that need reopening
    """
    submitted_uris = set()
    
    for timesheet in timesheet_data:
        if timesheet.get('timesheet_status') != 'Not Submitted':
            timesheet_uri = timesheet.get('timesheet_uri')
            if timesheet_uri:
                submitted_uris.add(timesheet_uri)
    
    return list(map(lambda ts: {
        'ts_uri': ts
    }, list(submitted_uris)))

def get_oef_and_tags_details(dag_run) -> Dict[str, str]:
    """
    Determine appropriate Object Extension Field (OEF) configuration for time entry.
    
    Matches the worktype from entry data against available OEF tag configurations
    to determine which OEF and tag URIs should be used for the time entry.
    Supports multiple worktype categories: standard, tarif, and tariffrei.

    Args:
        dag_run: Airflow DAG run object containing entry data and OEF configurations

    Returns:
        Dict[str, str]: Dictionary containing 'oef_uri' and 'oef_tag_uri' keys,
                        or empty dictionary if no matching worktype found
    """
    worktype = dag_run.conf['work_type']
    worktype_oef_tag_uri = rail.find_first_by_attr_and_get_attr(
        dag_run.conf['worktype_oef_tags'], 'name', worktype, 'uri', False)
    worktype_tarif_oef_tag_uri = rail.find_first_by_attr_and_get_attr(
        dag_run.conf['worktype_tarif_oef_tags'], 'name', worktype, 'uri', False)
    worktype_tariffrei_oef_tag_uri = rail.find_first_by_attr_and_get_attr(
        dag_run.conf['worktype_tariffrei_oef_tags'], 'name', worktype, 'uri', False)

    result = {}

    if worktype_oef_tag_uri:
        result['oef_uri'] = dag_run.conf['worktype_oef']
        result['oef_tag_uri'] = worktype_oef_tag_uri
    elif worktype_tarif_oef_tag_uri:
        result['oef_uri'] = dag_run.conf['worktype_tarif_oef']
        result['oef_tag_uri'] = worktype_tarif_oef_tag_uri
    elif worktype_tariffrei_oef_tag_uri:
        result['oef_uri'] = dag_run.conf['worktype_tariffrei_oef']
        result['oef_tag_uri'] = worktype_tariffrei_oef_tag_uri

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

    unique_row_combination = list(map(lambda x: {
        'unique_combination': f"{x['properties'].get('employee_id', '')}|{x['properties'].get('entry_date', '')}|{x['properties'].get('project_id', '')}|{x['properties'].get('task_name', '')}|{x['properties'].get('activity', '')}"
        }, log_records))

    final_data = list({f"{value['unique_combination']}": value for value in unique_row_combination}.values())

    #pylint: disable=cell-var-from-loop
    for item in final_data:
        entry_logs = list(
            filter(lambda x: 
                   (x['properties'].get('employee_id', '') == item['unique_combination'].split('|')[0]) and 
                   (x['properties'].get('entry_date', '') == item['unique_combination'].split('|')[1]) and 
                   (x['properties'].get('project_id', '') == item['unique_combination'].split('|')[2]) and 
                   (x['properties'].get('task_name', '') == item['unique_combination'].split('|')[3]) and
                   (x['properties'].get('activity', '') == item['unique_combination'].split('|')[4]), log_records))
        if len(entry_logs) > 0:
            first = entry_logs[0]
            final_log_records.append({
                'employee_id': first['properties']['employee_id'],
                'entry_date': first['properties']['entry_date'],
                'project_id': first['properties']['project_id'],
                'task_name': first['properties']['task_name'],
                'activity': first['properties']['activity'],
                'action': first['properties']['action'],
                'status': get_log_status(entry_logs),
                "details":  '; '.join(list(map(lambda x: x['properties'].get('details'), entry_logs))),
                'ecid': first['ecid'],
            })

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: get_status(x, 'skipped'), final_log_records ))))

    return  final_log_records

def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.max
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def validate_entry_date_format():
    """
    Validates entry date format in records loaded from get_all_records_for_user.
    
    Retrieves records from Airflow XCom, validates that entry_date fields
    follow the DD/MM/YYYY format, and separates valid and invalid records.
    Also extracts unique valid entry dates.
    
    Returns:
        dict: {
            'valid_records': [...],  # Records with correctly formatted dates
            'invalid_records': [...],  # Records with incorrectly formatted dates
            'unique_entry_dates': [...] # Unique valid entry dates with employee IDs
        }
    """
    records = rail.load_all_records(rail.result('get_all_records_for_user'))
    valid_records = []
    invalid_records = []

    startdate = rail.result('get_user_details')['userDetails']['employmentDateRange']['startDate'] if rail.result('get_user_details')['userDetails']['employmentDateRange'] else None

    user_startdate = get_date_from_replicon_date(startdate)
    
    # Track unique entry dates by employee
    unique_dates = {}
    
    for record in records:
        has_in_out_times = False
        # Skip if entry_date is missing
        if 'entry_date' not in record or not record['entry_date']:
            invalid_records.append(record)
            continue
            
        try:
            # Try to parse the date in the expected format
            entry_dt = datetime.strptime(record['entry_date'], config.entry_dateformat)
            if entry_dt < user_startdate:
                invalid_records.append(record)
                continue
            valid_records.append(record)

            # Check if in_time and out_time have values
            in_time = record.get('in_time')
            out_time = record.get('out_time')
            if in_time and out_time:
                has_in_out_times = True

            # Track unique entry date by employee_id
            emp_id = record.get('employee_id', '')
            entry_date = record.get('entry_date', '')
            unique_key = f"{emp_id}_{entry_date}"
            
            if has_in_out_times and (unique_key not in unique_dates):
                unique_dates[unique_key] = {
                    **record
                }
                
        except ValueError:
            # If parsing fails, consider it invalid
            invalid_records.append(record)
    
    return {
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'unique_entry_dates': list(unique_dates.values())
    }

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