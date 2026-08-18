from datetime import datetime, timedelta
from pendulum import now
from functools import lru_cache
import rail

null = None

SQL_DATEFORMAT = "%Y-%m-%d"
REP_DATE_FORMAT = "%m/%d/%Y"

# Timesheet Status Constants
TS_STATUS_APPROVED = "Approved"
TS_STATUS_WAITING_APPROVAL = "Waiting For Approval"
TS_STATUS_NOT_SUBMITTED = "Not Submitted"
TS_STATUS_REJECTED = "Rejected"

# Entry Status Constants
ENTRY_STATUS_APPROVED = "Approved"
ENTRY_STATUS_WAITING_APPROVAL = "Waiting For Approval"
ENTRY_STATUS_NOT_SUBMITTED = "Not Submitted"
ENTRY_STATUS_REJECTED = "Rejected"

# Valid statuses for reopening timesheets
REOPEN_ELIGIBLE_STATUSES = [TS_STATUS_WAITING_APPROVAL, TS_STATUS_APPROVED]
ENTRY_REOPEN_ELIGIBLE_STATUSES = [ENTRY_STATUS_WAITING_APPROVAL, ENTRY_STATUS_NOT_SUBMITTED]

# Log Status Constants
LOG_STATUS_SUCCESS = "Success"
LOG_STATUS_ERROR = "Error"
LOG_STATUS_EXCEPTION = "Exception"

def get_date(entry_date, date_format):
    """Convert Workday date format to SQL format YYYY-MM-DD"""
    try:
        if entry_date and datetime.strptime(entry_date, date_format):
            return datetime.strftime(datetime.strptime(entry_date, date_format), SQL_DATEFORMAT)
    except ValueError:
        return null
    return null

def is_date_within_range(date_str, date_format, days_range=90):
    """
    Check if date is within ±days_range from today.
    
    Args:
        date_str: Date string to validate
        date_format: Format of the date string
        days_range: Number of days for the range (default 90)
    
    Returns:
        bool: True if date is within range, False otherwise
    """
    try:
        if not date_str:
            return False
        date_obj = datetime.strptime(date_str, date_format)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        min_date = today - timedelta(days=days_range)
        max_date = today + timedelta(days=days_range)
        return min_date <= date_obj <= max_date
    except ValueError:
        return False


def get_validation_error_message(item):
    """Validate a CSV record for required fields and business rules"""
    from eisner_amper.timeoff_import_workday import config
    
    missing_fields = []
    required_fields = {
        'employeeid': 'employee_id',
        'Start Date': 'start_date',
        'Time Off Unit': 'hours',
        'Time Off Type Project Code': 'project_code',
        'Booking Reference ID': 'booking_reference_id'
    }

    for field_name, field_key in required_fields.items():
        if not item[field_key]:
            missing_fields.append(f"{field_name} value is missing")
            continue
        if field_key == "hours":
            try:
                hours_val = float(item[field_key])
                if hours_val < 0:
                    missing_fields.append(f"{field_name} are not valid (negative value)")
                # Allow 0 hours for DELETE operations
            except (ValueError, TypeError):
                missing_fields.append(f"{field_name} is not a valid number")
        if field_key == "start_date" and item["start_date"]:
            # Check date format
            if not get_date(item["start_date"], config.ENTRY_DATE_FORMAT):
                missing_fields.append("Start date is not valid, the expected format is MM/DD/YYYY")
            # Check date range
            elif not is_date_within_range(item["start_date"], config.ENTRY_DATE_FORMAT, 90):
                missing_fields.append("Start date must be within +/- 90 days from today")

    return "; ".join(missing_fields)

def get_unique_employee_ids():
    """
    Extract unique employee IDs from validated records.
    
    Returns:
        List[Dict]: List of dicts with employee_id key for parallel processing
    """
    valid_records = rail.load_json_artifact(rail.result('validate_and_split')['valid_records'])
    
    # Get unique employee IDs
    unique_employees = set()
    for record in valid_records:
        if record.get('employee_id'):
            unique_employees.add(record['employee_id'])
    
    # Return in format expected by parallel processing
    return [{'employee_id': emp_id} for emp_id in sorted(unique_employees)]

def get_records_for_employee(dag_run):
    """
    Get all valid records for a specific employee.
    
    Args:
        dag_run: The dag run context containing valid records
        
    Returns:
        List[Dict]: All valid records for the employee
    """
    valid_records = rail.load_json_artifact(dag_run.conf['valid_records'])
    return [record for record in valid_records if record.get('employee_id') == dag_run.conf['employee_id']]

@lru_cache(maxsize=128)
def get_work_location_oef_values(dag_run):
    return rail.load_json_artifact(dag_run.conf['work_location_oef_values'])

def validate_and_split_records():
    """
    Validate all records and split into valid and invalid lists with validation messages.
    
    Returns:
        dict: Contains 'valid_records', 'invalid_records' lists and validation messages
    """
    all_records = rail.load_all_records(rail.result("create_csv_collection"))
    valid_records = []
    invalid_records = []
    validation_logs = []
    
    for record in all_records:
        # Get validation error message for this record
        validation_msg = get_validation_error_message(record)
        
        if validation_msg:  # Has validation errors
            invalid_records.append(record)
            # Create log entry for this invalid record
            validation_logs.append({
                'booking_reference_id': record.get('booking_reference_id', ''),
                'employee_id': record.get('employee_id', ''),
                'start_date': record.get('start_date', ''),
                'hours': record.get('hours', ''),
                'project_code': record.get('project_code', ''),
                'status': 'Exception',
                'action': 'Validation',
                'details': validation_msg
            })
        else:  # Valid record
            valid_records.append(record)
    
    return {
        'valid_records': rail.write_json_artifact(valid_records),
        'invalid_records': rail.write_json_artifact(invalid_records),
        'validation_logs': rail.write_json_artifact(validation_logs),
        'has_invalid': len(invalid_records) > 0,
        'has_valid': len(valid_records) > 0
    }

def get_email_log_details(log_file_path, dag_run, STANDARD_EMAIL_DATE_FORMAT):
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
    current_time = now()
    start_time_str = dag_run.conf['start_time']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).total_seconds() / 60, 1),
        "log_file_name": dag_run.conf['log_filename'],
        "log_filepath": log_file_path,
        "input_filename": dag_run.conf['source_filename'],
        "total_record_count": dag_run.conf['total_record_count']
    }

def categorize_user_records_and_timesheets():
    """
    Categorize timesheets and their associated records by status using enriched data.
    
    Now uses entry-level status checks to determine which timesheets need reopening:
    - Waiting For Approval timesheets: Only reopen if entry is not present or is Waiting For Approval
    - Approved timesheets: Only reopen if entry is not present (new entry)
    
    Returns:
        Dict: Categorized timesheet and record information
    """
    timesheet_data = rail.result('get_timesheet_details')
    all_user_records = rail.result('enrich_user_records_with_entry_details')
    
    # Track unique timesheets and their records
    timesheets_to_reopen = set()
    blocked_records = []
    processable_records = []
    
    # Create a map of start_date to timesheet info
    date_to_timesheet = {}
    for ts_entry in timesheet_data:
        start_date = ts_entry.get('start_date')
        if start_date and start_date not in date_to_timesheet:
            date_to_timesheet[start_date] = {
                'timesheet_uri': ts_entry.get('timesheet_uri'),
                'timesheet_status': ts_entry.get('timesheet_status')
            }
    
    # Process each input record with enriched entry data
    for record in all_user_records:
        start_date = record.get('start_date')
        hours = float(record.get('hours', 0))
        existing_entry_uri = record.get('existing_entry_uri')
        existing_entry_status = record.get('existing_entry_status', '')
        
        # Look up timesheet info for this date
        ts_info = date_to_timesheet.get(start_date, {})
        ts_status = ts_info.get('timesheet_status')
        ts_uri = ts_info.get('timesheet_uri')
        
        # Determine if record can be processed based on entry and timesheet status
        can_process = False
        needs_timesheet_reopen = False
        block_reason = ''
        
        # Case 1: DELETE operation (hours = 0)
        if hours == 0:
            if not existing_entry_uri:
                # Cannot delete non-existent entry - this should be caught in validation
                # but adding here for completeness
                can_process = False
                block_reason = 'Cannot delete - time entry does not exist'
            elif existing_entry_status == ENTRY_STATUS_APPROVED:
                # Cannot delete approved entry
                can_process = False
                block_reason = 'Cannot delete approved time entry'
            elif existing_entry_status == ENTRY_STATUS_REJECTED and ts_status == TS_STATUS_REJECTED:
                # Cannot process rejected entry on rejected timesheet
                can_process = False
                block_reason = 'Cannot delete rejected time entry on rejected timesheet'
            else:
                # Can delete existing non-approved entry
                can_process = True
                # Check if timesheet needs reopening based on both entry and timesheet status
                if ts_uri and ts_status in REOPEN_ELIGIBLE_STATUSES:
                    # Need to reopen if entry is Waiting For Approval OR Not Submitted
                    if existing_entry_status in ENTRY_REOPEN_ELIGIBLE_STATUSES:
                        needs_timesheet_reopen = True
                    
        # Case 2: CREATE operation (no existing entry)
        elif not existing_entry_uri:
            if ts_status == TS_STATUS_APPROVED and ts_uri:
                # New entry on approved timesheet - need to reopen
                can_process = True
                needs_timesheet_reopen = True
            elif ts_status == TS_STATUS_WAITING_APPROVAL and ts_uri:
                # New entry on submitted timesheet - need to reopen
                can_process = True
                needs_timesheet_reopen = True
            else:
                # New entry on open/rejected timesheet - no reopen needed
                can_process = True
                
        # Case 3: UPDATE operation (existing entry)
        else:
            if existing_entry_status == 'Approved':
                # Cannot update approved entry
                can_process = False
                block_reason = 'Cannot update approved time entry'
            elif existing_entry_status == ENTRY_STATUS_REJECTED and ts_status == TS_STATUS_REJECTED:
                # Cannot process rejected entry on rejected timesheet
                can_process = False
                block_reason = 'Cannot update rejected time entry on rejected timesheet'
            else:
                # Can update non-approved entry
                can_process = True
                # Check if timesheet needs reopening based on both entry and timesheet status
                if existing_entry_status == ENTRY_STATUS_WAITING_APPROVAL and ts_uri and ts_status in REOPEN_ELIGIBLE_STATUSES:
                    needs_timesheet_reopen = True
        
        # Add to appropriate list
        if can_process:
            processable_records.append({
                **record,
                'timesheet_uri': ts_uri,
                'timesheet_status': ts_status
            })
            
            # Track timesheet operations
            if ts_uri and needs_timesheet_reopen:
                timesheets_to_reopen.add(ts_uri)
        else:
            blocked_records.append({
                **record,
                'timesheet_uri': ts_uri,
                'timesheet_status': ts_status,
                'block_reason': block_reason
            })
    
    return {
        'timesheets_to_reopen': [{'ts_uri': uri} for uri in timesheets_to_reopen],
        'timesheets_to_submit': [{'ts_uri': uri} for uri in timesheets_to_reopen],  # Same as reopen list
        'blocked_records': blocked_records,
        'processable_records': processable_records,
        'has_blocked_records': len(blocked_records) > 0
    }

def do_format_logs(dag_run):
    """Format logs for output"""
    formatted_logs = []
    logs = dag_run.conf['timeentrylogs'] + [dag_run.conf['otherlogs']]

    if logs:
        for timeentry in logs:
            log_records = rail.load_all_records(timeentry)
            for log in log_records:
                if isinstance(log, dict) and 'properties' in log:
                    formatted_logs.append(
                        {**log['properties'], "ecid": log.get("ecid", "")})
    main_logs = rail.load_all_records(rail.result(
        "create_main_log")) if rail.result("create_main_log") else []
    for log in main_logs:
        if isinstance(log, dict) and 'properties' in log:
            formatted_logs.append(
                {**log['properties'], "ecid": log.get("ecid", "")})

    success_count = len(
        list(filter(lambda log: log.get('status') == LOG_STATUS_SUCCESS, formatted_logs)))
    error_count = len(
        list(filter(lambda log: log.get('status') == LOG_STATUS_ERROR, formatted_logs)))
    exception_count = len(
        list(filter(lambda log: log.get('status') == LOG_STATUS_EXCEPTION, formatted_logs)))

    return {
        'logs': rail.write_json_artifact(formatted_logs),
        'success_count': success_count,
        'error_count': error_count,
        'exception_count': exception_count,
        'total_count': len(formatted_logs)
    }

def convert_date_dict_to_datetime(date_dict):
    """
    Convert Replicon date dictionary to datetime object.
    
    Args:
        date_dict: Dictionary with 'year', 'month', 'day' keys
        
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

@lru_cache(maxsize=128)
def get_effective_user_location():
    """
    Get the effective location URI for the user based on current date.
    
    Processes the locationSchedule from user details to find the currently
    effective location. The logic follows the same pattern as timesheet
    policy effective dates:
    - If effectiveDate is null, this is the initial location
    - Returns the location with the latest effectiveDate that is <= today
    
    Returns:
        str: The URI of the user's current effective location, or None if not found
    """
    user_details = rail.result('get_user_details')
    
    if not user_details:
        return None
    
    location_schedule = user_details.get('locationSchedule', [])
    
    if not location_schedule:
        return None
    
    # Get current date for comparison
    current_date = now()
    
    # Sort locations by effectiveDate (null dates first, then by date)
    sorted_locations = sorted(
        location_schedule,
        key=lambda x: (
            x.get('effectiveDate') is not None,
            convert_date_dict_to_datetime(x.get('effectiveDate')) if x.get('effectiveDate') else datetime.min
        )
    )
    
    effective_location = None
    
    for location in sorted_locations:
        effective_date = convert_date_dict_to_datetime(location.get('effectiveDate'))
        
        # Initial location (no effectiveDate) - use as default
        if effective_date is None:
            effective_location = location
        # Location with effectiveDate - check if it's current
        elif effective_date.date() <= current_date.date():
            effective_location = location
        else:
            # Future dated location - stop here
            break
    
    # Return the displayText and URI of the effective location
    if effective_location and 'location' in effective_location:
        location = effective_location['location']
        return {
            'displayText': location.get('displayText'),
            'uri': location.get('uri')
        }
    
    return None


def enrich_records_with_entry_details(valid_records, time_entries_data):
    """
    Enrich valid records with existing time entry details.
    
    Uses composite key of booking_reference_id + employee_id + entry_date
    to match existing entries and append their details to input records.
    
    Args:
        valid_records: List of validated input records
        time_entries_data: Time entry data from Replicon API
        
    Returns:
        List[Dict]: Records enriched with time entry details
    """
    # Create a lookup map using booking_reference_id as the unique key
    # Since booking reference ID is unique per entry, we only need that for matching
    entry_lookup = {}
    if time_entries_data:
        for entry_data in time_entries_data:
            if entry_data and 'entries' in entry_data:
                for entry in entry_data['entries']:
                    # Use only booking reference ID as the unique identifier
                    booking_ref = entry.get('unique_id', '')
                    if booking_ref:  # Only add if booking ref exists
                        entry_lookup[booking_ref] = {
                            'entry_uri': entry.get('entry_uri'),
                            'approval_status': entry.get('approval_status'),
                            'unique_id': entry.get('unique_id'),
                            'project_uri': entry.get('project_uri'),
                            'task_uri': entry.get('task_uri'),
                            'total_hours': entry.get('total_hours')
                        }
    
    # Enrich each record with entry details
    enriched_records = []
    for record in valid_records:
        # Use only booking reference ID for lookup
        booking_ref = record.get('booking_reference_id', '')
        
        existing_entry = entry_lookup.get(booking_ref, {})
        
        enriched_record = {
            **record,
            'existing_entry_uri': existing_entry.get('entry_uri'),
            'existing_entry_status': existing_entry.get('approval_status', ''),
            'existing_entry_hours': existing_entry.get('total_hours', 0)
        }
        enriched_records.append(enriched_record)
    
    return enriched_records
