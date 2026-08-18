"""
Custom methods for T-Systems ICT India Time Off Import
"""

import json
import re
from datetime import datetime, timedelta
from pendulum import now
import rail
import logging

DATE_FORMAT = "%d.%m.%Y"
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

def format_logs_for_output(log_records, config):
    """
    Format log records for CSV output
    
    Args:
        log_records (list): List of log records
        config: Configuration object
        
    Returns:
        list: Formatted log records
    """
    formatted_records = []
    
    for log in log_records:
        properties = log.get('properties', {})
        formatted_record = {
            'employee_id': properties.get('employee_id', ''),
            'transaction_id': properties.get('transaction_id', ''),
            'booking_start_date': properties.get('booking_start_date', ''),
            'booking_end_date': properties.get('booking_end_date', ''),
            'timeoff_type': properties.get('timeoff_type', ''),
            'booking_type': properties.get('booking_type', ''),
            'action': properties.get('action', ''),
            'status': log.get('severity', ''),
            'details': log.get('message', ''),
            'job_id': log.get('ecid', ''),
            'timestamp': log.get('timestamp', '')
        }
        formatted_records.append(formatted_record)
    
    return formatted_records

def do_format_logs(dag_run):
    """
    Format and aggregate logs from master and child DAGs
    
    Args:
        dag_run: Airflow DAG run context
        
    Returns:
        list: Formatted log records with statistics
    """
    log_artifacts = []
    log_records = []

    # Gather logs from child DAGs
    child_logs = rail.result('gather_timeoff_logs') if rail.result('gather_timeoff_logs') else []
    master_logs = rail.result('create_master_log') if rail.result('create_master_log') else []

    # Combine all log artifacts
    if child_logs:
        if isinstance(child_logs, list):
            log_artifacts.extend(child_logs)
        else:
            log_artifacts.append(child_logs)

    if master_logs:
        if isinstance(master_logs, list):
            log_artifacts.extend(master_logs)
        else:
            log_artifacts.append(master_logs)

    # Extract records from log artifacts
    if log_artifacts:
        for log in log_artifacts:
            records = rail.load_all_records(log)
            if records:
                log_records.extend(records)

    # Format records for output
    formatted_records = format_logs_for_output(log_records, None)

    # Calculate statistics
    success_count = len([r for r in formatted_records if r['status'] == 'Success'])
    error_count = len([r for r in formatted_records if r['status'] == 'Error'])
    exception_count = len([r for r in formatted_records if r['status'] == 'Exception'])
    total_count = len(formatted_records)

    # Set results for use in templates
    rail.set_result(key="success_record_count", val=success_count)
    rail.set_result(key="error_record_count", val=error_count)  
    rail.set_result(key="exception_record_count", val=exception_count)
    rail.set_result(key="total_record_count", val=total_count)

    return formatted_records

def get_email_details(timezone, log_file_path):
    """
    Generate email details for notifications
    
    Args:
        dag_run: Airflow DAG run context
        timezone (str): Timezone for timestamp
        
    Returns:
        dict: Email details including timestamps and file names
    """
    current_time = now(timezone)
    start_time_str = rail.result('log_start_time')
    filename = rail.render_template("{{ result('new_file_sensor') | file_base }}")
    return {
        "job_end_time": current_time.isoformat(),
        "job_duration_minutes": (((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).seconds)//60),
        "log_timestamp": current_time.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "log_file_name": f'Log_{filename}.csv',
        "log_filepath": log_file_path
    }

def get_required_timeoff_details(item, timeoff_mapper):
    """
    Get required time off details from the item.
    """
    replicon_timeoff_details = list(filter(lambda x: x['client_timeoff_type_name'] == item.get('time_off_type'),
        timeoff_mapper))
    if not replicon_timeoff_details:
        return None
    replicon_timeoff_detail = replicon_timeoff_details[0]['replicon_timeoff_type_name']

    return list(filter(lambda x:(x['displayText']).lower()== replicon_timeoff_detail.lower(), rail.result('get_timeoff_type_details')))

def get_date(date):
    if not date:
        return None
    year = date['year']
    month = date['month']
    day = date['day']
    return str(day).zfill(2) + '.'+str(month).zfill(2)+'.'+ str(year)


def validate_dates(dag_run):
    user_start_date = rail.result('get_user_info')[0]['start_date']
    user_end_date = rail.result('get_user_info')[0]['end_date']

    if user_start_date:
        format_userstartdate = datetime.strptime(
            get_date(user_start_date), DATE_FORMAT)
        format_timeoffstartdate = datetime.strptime(
            dag_run.conf['booking_start_date'], DATE_FORMAT)
        if user_end_date:
            format_userenddate = datetime.strptime(
            get_date(user_end_date), DATE_FORMAT)
            format_timeoffenddate = datetime.strptime(
                dag_run.conf['booking_end_date'], DATE_FORMAT)
            return (format_userstartdate <= format_timeoffstartdate) and (format_userenddate >= format_timeoffenddate)
        return format_userstartdate <= format_timeoffstartdate
    return False

def get_invalid_datetime_exception(dag_run):
    user_start_date = rail.result('get_user_info')[0]['start_date']
    user_end_date = rail.result('get_user_info')[0]['end_date']
    msg = ''
    if user_start_date:
        format_userstartdate = datetime.strptime(
            get_date(user_start_date), DATE_FORMAT)
        format_timeoffstartdate = datetime.strptime(
            dag_run.conf['booking_start_date'], DATE_FORMAT)
        if (format_userstartdate > format_timeoffstartdate):
            msg+= "Timeoff Booking Start Date is prior to User Start Date;"
        if user_end_date:
            format_userenddate = datetime.strptime(
            get_date(user_end_date), DATE_FORMAT)
            format_timeoffenddate = datetime.strptime(
                dag_run.conf['booking_end_date'], DATE_FORMAT)
            if format_userenddate < format_timeoffenddate:
                msg+= "User End Date is prior to Timeoff Booking End Date"
    return msg
