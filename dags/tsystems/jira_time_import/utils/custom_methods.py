from datetime import datetime
from pendulum import now
import rail

null = None

SQL_DATEFORMAT = "%Y-%m-%d"
REP_DATE_FORMAT = "%m/%d/%Y"

def get_date(entry_date, date_format):
    """Convert T-Systems date format to SQL format YYYY-MM-DD"""
    try:
        if entry_date and datetime.strptime(entry_date, date_format):
            return datetime.strftime(datetime.strptime(entry_date, date_format), SQL_DATEFORMAT)
    except ValueError:
        return null
    return null

def get_processed_import_records(ENTRY_DATE_FORMAT):
    input_data = rail.load_all_records(rail.result("create_csv_collection"))
    return rail.write_json_artifact([
        {
            **item,
            "task_name": item["full_task_path"].split("|")[-1].strip(),
            "entry_date_sql": get_date(item["entry_date"], ENTRY_DATE_FORMAT)
        } for item in input_data
    ])

def get_validation_error_message(item):
    """Validate a CSV record for required fields"""
    missing_fields = []
    required_fields = {
        'ID': 'unique_id',
        'Login': 'employee_id',
        'FECHA_WORK': 'entry_date',
        'HORAS': 'hours',
        'PEP': 'project_id',
        'TAREA': 'full_task_path'
    }

    for field_name, field_key in required_fields.items():
        if not item[field_key]:
            missing_fields.append(f"{field_name} value is missing")
            continue
        if field_key == "hours" and float(item[field_key]) < 0:
            missing_fields.append(f"{field_name} are not valid")
        if field_key == "entry_date" and item["entry_date"] and not item["entry_date_sql"]:
            missing_fields.append("Entry date is not valid, the expected format is DD/MM/YYYY")

    return "; ".join(missing_fields)

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

def get_submitted_timesheet_uris():
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
    timesheet_data = rail.result('get_timesheet_details')
    submitted_uris = set()
    
    for timesheet in timesheet_data:
        if timesheet.get('timesheet_status') != 'Not Submitted':
            timesheet_uri = timesheet.get('timesheet_uri')
            if timesheet_uri:
                submitted_uris.add(timesheet_uri)
    
    return list(map(lambda ts: {
        'ts_uri': ts
    }, list(submitted_uris)))

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
        list(filter(lambda log: log.get('status') == 'Success', formatted_logs)))
    error_count = len(
        list(filter(lambda log: log.get('status') == 'Error', formatted_logs)))
    exception_count = len(
        list(filter(lambda log: log.get('status') == 'Exception', formatted_logs)))

    return {
        'logs': formatted_logs,
        'success_count': success_count,
        'error_count': error_count,
        'exception_count': exception_count,
        'total_count': len(formatted_logs)
    }
