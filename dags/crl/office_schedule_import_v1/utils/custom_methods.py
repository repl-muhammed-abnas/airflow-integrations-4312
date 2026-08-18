"""
Custom utility methods for CRL Office Schedule Sync
"""
import rail
from datetime import datetime
import itertools
from pendulum import now


def get_validation_error_properties(item):
    """
    Generate validation error properties for logging

    Args:
        item: Dictionary containing schedule data

    Returns:
        Dictionary with error details
    """
    schedule_name = item.get('schedule_name', '')
    pattern = item.get('pattern', '')
    start_date = item.get('start_date', '')

    errors = []

    # Check missing schedule name
    if not schedule_name or not schedule_name.strip():
        errors.append("ScheduleName not present in the payload")

    # Check missing pattern
    if not pattern or not pattern.strip():
        errors.append("Pattern not present in the payload")
    elif is_non_standard_pattern(pattern) and (not start_date or not start_date.strip()):
        errors.append(
            "StartDate required for non-7-day patterns but not present")

    error_message = "; ".join(errors) if errors else "Validation failed"

    return {
        'schedule_name': schedule_name,
        'pattern': pattern,
        'start_date': start_date,
        'action': 'Validation',
        'status': 'Exception',
        'details': 'Office Schedule not processed as -' + error_message
    }


def page_handler(request, result):
    """Handle pagination for Replicon API responses."""
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return None


def filter_office_schedule_data(result):
    """Transform Replicon existing office schedule list response into simplified dictionary format."""
    rows_list = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))

    if not rows_list:
        return []

    return list(map(lambda item: {
        "existing_office_schedule_name": item['displayText'],
        "existing_office_schedule_uri": item['uri']}, rows_list)) if rows_list else []


def get_process_dag_ids(parallel_count, trigger_task_id):
    """Collect all DAG run IDs from parallel trigger tasks."""
    dag_ids = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'{trigger_task_id}_{x+1}') if rail.result(
            f'{trigger_task_id}_{x+1}') else []), range(parallel_count)))))

    return dag_ids


def validate_pattern_format(pattern):
    """
    Validate that pattern has correct format (any length >= 1)

    Args:
        pattern: Pipe-delimited pattern string

    Returns:
        Boolean indicating if pattern is valid
    """
    if not pattern or not pattern.strip():
        return False

    values = pattern.split('|')

    # Must have at least 1 value
    if len(values) < 1:
        return False

    # Check each value is either 'X' or a valid number
    for val in values:
        val = val.strip().upper()
        if val == 'X':
            continue
        try:
            hours = float(val)
            if hours < 0 or hours > 24:
                return False
        except ValueError:
            return False

    return True


def validate_schedule_data(pattern, start_date, date_format):
    """
    Validate pattern format and date format together

    Args:
        pattern: Pipe-delimited pattern string
        start_date: Start date string
        date_format: Expected date format (e.g., "%m/%d/%Y")

    Returns:
        Tuple of (is_valid, error_message)
    """
    errors = []

    # Validate pattern format
    if not validate_pattern_format(pattern):
        errors.append("Invalid pattern format")

    # Validate date format if start_date is provided and pattern is non-standard
    if start_date and start_date.strip():
        if not validate_date_format(start_date, date_format):
            errors.append(
                f"Invalid date format. Expected format: {date_format}")

    if errors:
        return (False, "; ".join(errors))
    return (True, None)


def is_non_standard_pattern(pattern):
    """
    Determine if pattern requires PutRecurringSchedulePattern API

    Args:
        pattern: Pipe-delimited pattern string

    Returns:
        True if pattern length != 7 (needs recurring API + StartDate)
        False if pattern length == 7 (uses simple API, no StartDate)
    """
    if not pattern:
        return False
    values = pattern.split('|')
    return len(values) != 7


def validate_date_format(date_str, date_format):
    """
    Validate if a date string matches the expected format

    Args:
        date_str: Date string to validate
        date_format: Expected format (e.g., "%m/%d/%Y")

    Returns:
        True if date matches format, False otherwise
    """
    if not date_str or not date_str.strip():
        return True  # Empty dates are handled separately
    try:
        datetime.strptime(date_str.strip(), date_format)
        return True
    except ValueError:
        return False


def parse_start_date(start_date_str, start_date_format):
    """
    Parse StartDate from MM/DD/YYYY format to year, month, day dict

    Args:
        start_date_str: Date string in MM/DD/YYYY format

    Returns:
        Dictionary with year, month, day as integers
    """
    date_obj = datetime.strptime(start_date_str.strip(), start_date_format)
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }


def parse_pattern_to_array(pattern):
    """
    Parse pipe-delimited pattern into array of float values

    Args:
        pattern: Pipe-delimited pattern string (e.g., "7.5|7.5|7.5|7.5|7.5|X|X")

    Returns:
        List of float values (X converted to 0.0)
    """
    values = pattern.split('|')
    result = []

    for val in values:
        val = val.strip().upper()
        result.append(0.0) if val == 'X' else result.append(float(val))

    return result


def convert_hours_to_duration(hours):
    """
    Convert decimal hours to Replicon duration format

    Args:
        hours: Decimal hours (e.g., 7.5)

    Returns:
        Dictionary with hours, minutes, seconds, milliseconds, microseconds
    """
    return {
        "hours": int(hours),
        "minutes": int((hours * 60) % 60),
        "seconds": int((hours * 60 * 60) % 60),
        "milliseconds": 0,
        "microseconds": 0
    }


def format_all_logs(dag_run):
    """
    Aggregate and format all logs from master and child DAGs

    Args:
        dag_run: Airflow DagRun object

    Returns:
        List of formatted log records with statistics
    """
    log_artifacts = []
    log_records = []

    # Gather logs from child DAGs
    creation_logs = dag_run.conf['child_logs']
    # Gather logs from master DAG
    master_logs = dag_run.conf['master_log']

    # Process creation logs
    if creation_logs:
        if isinstance(creation_logs, list):
            log_artifacts.extend(creation_logs)
        else:
            log_artifacts.append(creation_logs)

    # Process master logs
    if master_logs:
        if isinstance(master_logs, list):
            log_artifacts.extend(master_logs)
        else:
            log_artifacts.append(master_logs)

    # Load all log records
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    # Format log records
    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Skipped', final_log_records))))
    rail.set_result(key="total_record_count",
                    val=dag_run.conf['total_records'])

    return final_log_records


def get_email_details_callable(dag_run, time_zone):
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_start_time'], "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"Log_office_schedule_import_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }
