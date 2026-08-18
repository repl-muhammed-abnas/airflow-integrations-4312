
from datetime import datetime
import rail
import json
import csv
import tempfile
import pendulum
from io import StringIO
import os

def get_logging_details():
    today = pendulum.now()
    return {
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "log_filename": 'Timeoff_Sync_HIBOB_to_Polaris_Logs_' + today.strftime("%Y%m%d_%H%M%S") + '.csv'
    }

def do_format_logs():

    def get_filtered_records(logs, status):
        return list(filter(lambda log: log['status'].lower() == status, logs))

    def get_record_summary(logs):
        return {
            "success": len(get_filtered_records(logs, 'success')),
            "failed":  len(get_filtered_records(logs, 'error')),
            "skipped":  len(get_filtered_records(logs, 'skipped')),
            "exception": len(get_filtered_records(logs, "exception"))
        }

    master_log = json.loads(rail.result('load_master_log'))
    
    gather_logs = rail.result('gather_timeoff_logs') if rail.result('gather_timeoff_logs') else []

    for log in gather_logs:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)

    logs = []

    logs = list(map(lambda log: {
        **{
            'ecid': log['ecid']
        },
        **log['properties'],
        }, master_log))

    return {
        "get_record_summary": get_record_summary(logs),
        "final_logs": json.dumps(logs, ensure_ascii=False)
    }

    
def check_timeoff_type_assigned_to_user(dag_run):
    user_timeoff_policy_data = rail.result("get_user_details")["timeOffTypePolicySummary"]["policiesByTimeOffType"]
    return rail.find_first_by_attr_and_get_attr(user_timeoff_policy_data, "timeOffType.name", dag_run.conf["booking_data"]["timeofftypename"])

def filter_downloaded_csv_file():    
    file_content = rail.read_artifact(rail.result("download_sftp_file"))

    csv_io = StringIO(file_content)
    reader = list(csv.reader(csv_io))
    
    cleaned_rows = reader
    if len(reader) >= 2:
        first_row_vals = [cell.strip().lower() for cell in reader[0] if cell.strip()]
        second_row_vals = [cell.strip().lower() for cell in reader[1] if cell.strip()]
        all_header_vals = first_row_vals + second_row_vals

        has_from_date = any('from date' in val for val in all_header_vals)
        has_to_date = any('to date' in val for val in all_header_vals)
        
        if has_from_date and has_to_date:
            cleaned_rows = reader[2:]

    encoding = rail.result('find_file_encoding')[0]['encoding']
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding=encoding) as temp_file:
        writer = csv.writer(temp_file)
        writer.writerows(cleaned_rows)
        temp_file_path = temp_file.name
    with open(temp_file_path, 'r', encoding=encoding) as f:
        cleaned_content = f.read()
    
    os.unlink(temp_file_path)

    return cleaned_content

def get_action_to_be_performed(dag_run):
    if dag_run.conf['booking_data'].get('changetype').lower() == 'request approved' and dag_run.conf['booking_data'].get('status').lower() == 'approved':
        return 'update_status'
    elif dag_run.conf['booking_data'].get('changetype').lower() == 'request submitted' and dag_run.conf['booking_data'].get('status').lower() == 'pending approval':
        return 'add'
    elif dag_run.conf['booking_data'].get('changetype').lower() == 'request updated' and dag_run.conf['booking_data'].get('status').lower() == 'pending approval':
        return 'update'
    elif dag_run.conf['booking_data'].get('changetype').lower() == 'request cancelled' and dag_run.conf['booking_data'].get('status').lower() == 'cancelled':
        return 'delete'
    return ''

def get_email_log_details():
    current_time = pendulum.now()
    start_time_str = rail.result("logging_details")['process_start_time']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f%z')).total_seconds() / 60, 1),
        "log_file_name": rail.result('logging_details')['log_filename'],
        "input_filename": rail.render_template('{{ result("new_file_sensor_to_process") | file_name }}'),
        "total_record_count": rail.result("create_collection_from_csv", key="length")
    }