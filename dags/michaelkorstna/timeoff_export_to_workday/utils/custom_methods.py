from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowFailException
import pendulum
import rail

CONF_DATE_FORMAT = '%m/%d/%Y'


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_date_json(date_obj):
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }


def get_logging_details(time_zone, time_type, export_file_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    if time_type == 'New':
        export_end_date = today
        export_start_date = today
    else:  # delta
        export_end_date = today - relativedelta(days=1)
        export_start_date = None
    return {
        "current_time": current_time,
        "time_export_filename": f"{export_file_prefix}_{current_time}",
        "time_export_filename_nodata": f"{export_file_prefix}_{current_time}_Nodata",
        "export_start_date": export_start_date.strftime("%Y/%m/%d") if export_start_date else None,
        "export_end_date": export_end_date.strftime("%Y/%m/%d"),
        "export_start_date_json": get_date_json(export_start_date) if export_start_date else None,
        "export_end_date_json": get_date_json(export_end_date),
        "time_zone": time_zone
    }


def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']


def format_booking_date(dag_run):
    """Convert entrydate from MM/DD/YYYY to YYYYMMDD format"""
    dt_obj = datetime.strptime(dag_run.conf["entrydate"], "%m/%d/%Y")
    return dt_obj.strftime("%Y%m%d")


def create_unique_record_id(dag_run):
    """Create unique ID for a time-off record"""
    return f"Replicon_{dag_run.conf['timeoffbookingid']}_{rail.result('format_booking_date')}_{dag_run.conf['timeofftypedescription']}"


def extract_reason_from_timeoff_details():
    """Extract reason text value from extension fields"""
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_timeoff_details')['extensionFieldValues'],
        'definition.displayText',
        'Reason',
        'textValue'
    )


def format_date_yyyy_mm_dd(date_str):
    """Convert date from MM/DD/YYYY to YYYY-MM-DD format"""
    if isinstance(date_str, datetime):
        return date_str.strftime("%Y-%m-%d")
    dt_obj = datetime.strptime(date_str, "%m/%d/%Y")
    return dt_obj.strftime("%Y-%m-%d")


def generate_log_filename(company_key, job_time):
    """Generate the log filename"""
    return f"{company_key}_timeoffexportlog_{job_time}.csv"


def load_log_records(log_artifact):
    """
    Load all records from a log artifact.

    Args:
        log_artifact: Log artifact reference string

    Returns:
        List of all records from the log artifact
    """
    return rail.load_all_records(log_artifact)


def format_logs(dag_run):
    """
    Process and format logs from all processing activities.

    Consolidates logs from log artifacts and counts records by status (Error, Exception, Success).
    Sets counts as separate results using rail.set_result().
    Returns artifact reference to avoid large xcom/log data.

    Args:
        dag_run: Airflow DAG run object containing log artifact references

    Returns:
        str: Artifact reference containing formatted log records
    """
    log_artifacts = dag_run.conf.get('logs', [])
    log_records = []

    # Load records from each log artifact
    if log_artifacts:
        for log_artifact in log_artifacts:
            if log_artifact:
                records = load_log_records(log_artifact)
                if records:
                    log_records.extend(records)

    error_count = 0
    exception_count = 0
    success_count = 0

    formatted_logs = []

    for log in log_records:
        properties = log.get('properties', {})
        status = properties.get('status', '')

        if status == 'Error':
            error_count += 1
        elif status == 'Exception':
            exception_count += 1
        elif status == 'Success':
            success_count += 1

        # Split combined fields
        timeoff_type_combined = properties.get('timeofftypename|timeoffdescription', '|')
        timeoff_parts = timeoff_type_combined.split('|') if timeoff_type_combined else ['', '']

        transaction_combined = properties.get('transactiontype|childjob', '|')
        transaction_parts = transaction_combined.split('|') if transaction_combined else ['', '']

        formatted_logs.append({
            'employeeid': properties.get('employeeid', ''),
            'loginname': properties.get('loginname', ''),
            'timeoffbookingid': properties.get('timeoffbookingid', ''),
            'timeofftypename': timeoff_parts[0] if len(timeoff_parts) > 0 else '',
            'timeoffcode': timeoff_parts[1] if len(timeoff_parts) > 1 else '',
            'hours': properties.get('hours', ''),
            'entrydate': properties.get('entrydate', ''),
            'transactiontype': transaction_parts[0] if len(transaction_parts) > 0 else '',
            'status': status,
            'details': properties.get('details', ''),
            'ecid': log.get('ecid', '')
        })

    # Set counts as separate results for template access
    rail.set_result(key="error_count", val=error_count)
    rail.set_result(key="exception_count", val=exception_count)
    rail.set_result(key="success_count", val=success_count)
    rail.set_result(key="total_count", val=len(formatted_logs))

    # Write to artifact to avoid large xcom/log data
    return rail.write_json_artifact(formatted_logs)
