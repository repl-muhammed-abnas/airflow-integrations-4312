from pendulum import now
from datetime import datetime as py_datetime, timedelta
import rail
from airflow.models import Variable, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.session import NEW_SESSION, provide_session
from transparentbpo.timeoff_import import config

DATE_FORMAT = "%Y-%m-%d"

def get_email_details_callable(time_zone):
    """
    Generate email details with timestamp information.

    Args:
        time_zone: Timezone for timestamp generation

    Returns:
        Dictionary containing log_timestamp, email_timestamp, and log_file_name
    """
    _now = now(time_zone)
    return {
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"bamboohrtimeoffimportlog_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }

def add_timeoffs_array(item):
    """
    Add time_offs array to item by transforming dates dictionary.

    Args:
        item: Dictionary containing dates information

    Returns:
        Modified item with time_offs array
    """
    item['time_offs'] = [
        {"date": date, "hours": hours}
        for date, hours in item.get('dates', {}).items()
    ]
    return item

def manipulate_schedule_hrs(timeoff_item, schedule_list):
    """
    Find scheduled hours for a specific date from schedule list.

    Args:
        timeoff_item: Date string to search for
        schedule_list: List of schedule objects with date and hours

    Returns:
        Scheduled hours as string or empty string if not found
    """
    if len(schedule_list) < 1:
        return ""

    return rail.find_first_by_attr_and_get_attr(schedule_list, 'date', timeoff_item, 'scheduledTotalHours', '')

def do_get_last_run_date(config):
    """
    Get and update the last run date from Airflow variables.

    Args:
        config: Configuration object with last_run_date_var_name

    Returns:
        Previous last run date string
    """
    current_time = now()
    last_run_date = Variable.get(config.last_run_date_var_name, default_var="")
    Variable.set(config.last_run_date_var_name, current_time.strftime(DATE_FORMAT))
    return last_run_date

def get_endpoint_detail(config):
    """
    Generate BambooHR API endpoint with date range.
    Uses config.daterange as single integer for both start and end offset.

    Args:
        config: Configuration object with daterange value

    Returns:
        API endpoint string with date range parameters
    """
    current_time = now()
    start = current_time - timedelta(days=config.daterange)
    end = current_time + timedelta(days=config.daterange)
    endpoint = f"/time_off/requests?start={start.strftime(DATE_FORMAT)}&end={end.strftime(DATE_FORMAT)}&status={config.timeoff_status}"
    return endpoint

def filter_timeoff_by_type(response):
    """
    Filter BambooHR timeoff records to only include allowed timeoff types from config.

    Args:
        response: List of timeoff records from BambooHR

    Returns:
        Filtered list containing only allowed timeoff types
    """
    if not response:
        return []

    filtered_records = [
        record for record in response
        if record.get('type', {}).get('name') in config.allowed_timeoff_types
    ]

    return filtered_records

def get_timesheet_uri(response):
    """
    Extract timesheet URI from GetTimesheetForDate2 response.

    Args:
        response: Response from GetTimesheetForDate2 API call

    Returns:
        Timesheet URI string or None if not found
    """
    if not response:
        return None

    timesheet = response.get('timesheet', {})
    if not timesheet:
        return None

    return timesheet.get('uri', None)

def is_user_enabled(response):
    """
    Check if user exists and is enabled from BulkGetUsers3 response.

    Args:
        response: Response from BulkGetUsers3 API call

    Returns:
        Boolean indicating if user exists and is enabled
    """
    if not response:
        return False

    if not response[0]['userDetails']['isEnabled']:
        return False

    return True

def extract_user_uri(response):
    """
    Extract user URI from BulkGetUsers3 response.

    Args:
        response: Response from BulkGetUsers3 API call

    Returns:
        User URI string
    """
    return response[0]['userDetails']['uri']

def get_timeoff_uri_from_user_data(response, timeoff_type_name):
    """
    Extract timeoff type URI from BulkGetUsers3 response policiesByTimeOffType.

    Args:
        response: Response from BulkGetUsers3 API call
        timeoff_type_name: Name of the timeoff type to find

    Returns:
        URI string of timeoff type or empty string if not found
    """
    if not response:
        return ""

    timeoff_policies = response[0].get('timeOffTypePolicySummary', {}).get('policiesByTimeOffType', [])

    for policy in timeoff_policies:
        if policy.get('timeOffType', {}).get('name') == timeoff_type_name:
            return policy.get('timeOffType', {}).get('uri', "")

    return ""

def format_user_schedule_hrs_list(response):
    """
    Format scheduled hours response into simplified list structure.

    Args:
        response: Response from GetScheduledHoursInDateRange API call

    Returns:
        Dictionary with schedulelist array containing date and hours
    """
    result_list = []

    for item in response:
        day = item['date']['day']
        month = item['date']['month']
        year = item['date']['year']

        date_str = f"{year}-{month:02d}-{day:02d}"
        total_hours = item['scheduledTotalHours']['hours']

        result_list.append({
            "date": date_str,
            "scheduledTotalHours": str(total_hours)
        })

    return {
        "schedulelist": result_list
    }


def do_format_logs(dag_run):
    """
    Format log records for final processing.

    Args:
        dag_run: DagRun object containing log reference

    Returns:
        List of formatted log entry dictionaries
    """
    log_entries = []
    logs = rail.result("gather_child_logs")

    if logs:
        for timeentry in logs:
            log_records = rail.load_all_records(timeentry)
            for log in log_records:
                properties = log['properties']
                log_entries.append({
                    'timeoff_id': properties['timeoff_id'],
                    'bamboohr_id': properties['bamboohr_id'],
                    'employee_id': properties['employee_id'],
                    'username': properties['username'],
                    'timeoff_type': properties['timeoff_type'],
                    'booking_date': properties['booking_date'],
                    'status': properties['status'],
                    'details': properties['details'],
                    'ecid': log['ecid']
                })

    return log_entries

@provide_session
def get_dagruns_to_process(time_zone, master_dag_id, session=NEW_SESSION):
    """
    Query and return successful DAG runs from log pregeneration child DAGs.
    Fetches DAG runs that completed yesterday in the configured timezone.

    Args:
        config: Configuration object with lookup variables
        session: Database session (provided by decorator)

    Returns:
        List of DAG run IDs to process
    """
    current_time = now(time_zone)

    # Calculate yesterday's date range in configured timezone
    yesterday_start = current_time.subtract(days=1).start_of('day')
    yesterday_end = current_time.subtract(days=1).end_of('day')

    # Convert to UTC for database query (DagRun.end_date is stored in UTC)
    yesterday_start_utc = yesterday_start.in_tz('UTC')
    yesterday_end_utc = yesterday_end.in_tz('UTC')

    dag_runs_to_filter = (
        session.query(DagRun.id, DagRun.dag_id, DagRun.state, DagRun.end_date)
        .select_from(DagRun)
        .filter(
            DagRun.dag_id == master_dag_id,
            DagRun.state.in_([DagRunState.SUCCESS]),
            DagRun.end_date >= yesterday_start_utc,
            DagRun.end_date <= yesterday_end_utc)
        .group_by(DagRun.id, DagRun.dag_id, DagRun.state, DagRun.end_date)
        .all()
    )
    dag_runs = [item[0] for item in dag_runs_to_filter] if dag_runs_to_filter else []

    return dag_runs


def generate_dedup_key(record):
    """
    Generate deduplication key for a BambooHR timeoff record.
    Matches Workato connector dedup logic.

    Format: {id}-{lastChanged}-{typeId}-{amount}-{start}-{end}

    Args:
        record: BambooHR timeoff record

    Returns:
        Dedup key string
    """
    return (
        f"{record.get('id', '')}-"
        f"{record.get('status', {}).get('lastChanged', '')}-"
        f"{record.get('type', {}).get('id', '')}-"
        f"{record.get('amount', {}).get('amount', '')}-"
        f"{record.get('start', '')}-"
        f"{record.get('end', '')}"
    )


def add_dedup_key_to_record(record):
    """
    Add dedup key and processed date to a BambooHR timeoff record.

    Args:
        record: BambooHR timeoff record

    Returns:
        Modified record with dedup_key and processed_date fields
    """
    record['dedup_key'] = generate_dedup_key(record)
    record['processed_date'] = now(config.time_zone).strftime(DATE_FORMAT)
    return record


def get_cutoff_date(time_zone, retention_days):
    """
    Calculate the cutoff date for reference file cleanup.

    Args:
        time_zone: Timezone for date calculation
        retention_days: Number of days to retain records

    Returns:
        Cutoff date string in YYYY-MM-DD format
    """
    current_date = now(time_zone)
    cutoff_date = current_date.subtract(days=retention_days)
    return cutoff_date.strftime(DATE_FORMAT)


def get_new_changed_from_original():
    """
    Filter original BambooHR data to get only new/changed records.
    Uses dedup_keys from query_new_changed_records to filter original data.

    Returns:
        List of original BambooHR items (with dates) that are new or changed
    """
    original_data = rail.result('get_users_timeoff')
    new_changed_records = rail.load_all_records(rail.result('query_new_changed_records'))

    # Get dedup_keys from query result
    new_dedup_keys = {record['dedup_key'] for record in new_changed_records}

    # Filter original data to get items with matching dedup_keys
    return [
        item for item in original_data
        if generate_dedup_key(item) in new_dedup_keys
    ]


def reference_file_row_from_collection(item):
    """
    Map a collection item to a CSV row for reference file.

    Args:
        item: Dictionary from QueryCollectionOperator

    Returns:
        List of values for CSV row
    """
    return [
        item.get('id', ''),
        item.get('dedup_key', ''),
        item.get('processed_date', '')
    ]


def reference_file_row_from_bamboohr(item, time_zone):
    """
    Map a BambooHR timeoff record to a CSV row for reference file.

    Args:
        item: Dictionary with BambooHR response structure
        time_zone: Timezone for processed_date

    Returns:
        List of values for CSV row
    """
    return [
        item.get('id', ''),
        generate_dedup_key(item),
        now(time_zone).strftime(DATE_FORMAT)
    ]