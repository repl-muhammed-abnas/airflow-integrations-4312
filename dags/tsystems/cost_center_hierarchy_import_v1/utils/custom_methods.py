"""
Custom methods for the T-Systems Cost Center Hierarchy Import integration.
"""

from functools import lru_cache
import hashlib
import json
from datetime import datetime, timedelta
from pendulum import now
from rail import (result, write_json_artifact, load_all_records, smartjoin_by_delim, set_result)

from airflow.models import Variable


JOB_LOG_TIMESTAMP = "%Y-%m-%dT%H:%M:%S%z"
LOG_FILE_TIMESTAMP = "%Y%m%dT%H%M%S"

def get_email_details_callable(dag_run, timezone):
    _now = now(timezone)
    return {
        "job_end_time" : (_now).isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_started_time'], "%y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"Log_{dag_run.conf['process_file_name']}_{_now.strftime('%y%m%dT%H%M%S')}.csv"
    }

def can_run_batch_task_test(var_name):
    var = Variable.get(var_name, default_var='false')
    return var.lower() == 'true'

def compute_sha256_hash(item):
    if not item:
        return []
    """
    Compute SHA256 hash for a cost center record.
    
    Args:
        item: Dictionary containing cost center data
        
    Returns:
        Dictionary with SHA256 hash added
    """
    # Extract fields for hash computation
    hash_fields = {
        'Name': item.get('Name', ''),
        'Code': item.get('Code', '').lower(),
        'Description': item.get('Description', ''),
        'Status': item.get('Status', ''),
        'Cost_Center_Manager': item.get('Cost Center Manager', '')
    }

    cost_center_update_hash_fields = {
        'Name': item.get('Name', ''),
        'Code': item.get('Code', '').lower(),
        'Description': item.get('Description', ''),
        'Status': item.get('Status', ''),
    }
    
    # Convert to sorted JSON string and hash
    hash_str = json.dumps(hash_fields, sort_keys=True)
    hash_value = hashlib.sha256(hash_str.encode()).hexdigest()
    cost_center_update_hash_str = json.dumps(cost_center_update_hash_fields, sort_keys=True)
    cost_center_update_hash_val = hashlib.sha256(cost_center_update_hash_str.encode()).hexdigest()
    # Add hash to item
    result = item.copy()
    result['SHA256'] = hash_value
    result['CostCenterDetailsSHA256'] = cost_center_update_hash_val
    
    return result

def identify_missing_fields(item):
    """
    Identify which mandatory fields are missing from a cost center record.
    
    Args:
        item: Dictionary containing cost center data
        
    Returns:
        List of field names that are missing or empty
    """
    required_fields = ['Name', 'Code', 'Description', 'Status']
    missing = []
    
    for field in required_fields:
        if not item.get(field):
            missing.append(field)
            
    return missing

def get_invalid_log_message(item):
    """
    Generate an error message for invalid cost center records.
    
    Args:
        item: Dictionary containing cost center data
        
    Returns:
        String with error message
    """
    missing_fields = identify_missing_fields(item)
    
    if not missing_fields:
        return f"Cost center hierarchy is more than 7 levels deep for ({item.get('Code', 'Unknown')})."
    
    return f"Invalid cost center record: {', '.join(missing_fields)} field(s) are missing or empty."

def get_hierarchy_level(name):
    """
    Calculate the hierarchy level based on the Name field (pipe-separated path).
    
    Args:
        name: String containing pipe-separated path
        
    Returns:
        Integer representing hierarchy level (depth)
    """
    if not name:
        return 1
        
    # Split by pipe and count parts
    parts = name.split('|')
    return len(parts)

@lru_cache(maxsize=8)
def get_updated_departments(task_id):
    return write_json_artifact(result(task_id))


def _get_all_job_ids(logs):
    return smartjoin_by_delim(arr=[log['ecid'] for log in logs], separator="^^^")

def get_log_status(logs, status_counter):
    all_status = [log['properties']['status'].lower() for log in logs]
    if 'error' in all_status:
        status_counter['error'] +=1
        return 'Error'
    if 'exception' in all_status:
        status_counter['exception'] +=1
        return 'Exception'
    status_counter['success'] +=1
    return 'Success'

def _get_log_message(logs):
    return smartjoin_by_delim(arr=[log['properties']['details'] for log in logs if log['properties']['details']], separator="^^^")

def process_logs_and_filter_logs(dag_run):

    process_logs = dag_run.conf['logs'] or []
    process_logs.append(dag_run.conf['exception_logs'])

    final_logs = []
    status_counter = {
        'success': 0,
        'error': 0,
        'exception': 0
    }

    log_records = []
    unique_cost_codes = set()
    unique_managers = set()
    for log_artifact in process_logs:
        _data = load_all_records(log_artifact)
        if _data:
            log_records.extend(_data)
            for record in _data:
                # permission removal log records
                if record['properties']['code'] == "NA":
                    unique_managers.add(record['properties']['manager_id'])
                    continue
                if record['properties']['code'] not in unique_cost_codes:
                    unique_cost_codes.add(record['properties']['code'])

    for cost_code in unique_cost_codes:
        _all_logs = list(filter(lambda x: x['properties']['code'] == cost_code ,log_records))
        _first = _all_logs[0]
        final_logs.append(
            {
                "CostCenterCode": cost_code,
                "ManagerId": _first['properties']['manager_id'],
                "Action": _first['properties']['action'],
                "Status": get_log_status(_all_logs, status_counter),
                "Details": _get_log_message(_all_logs),
                "JobId": _get_all_job_ids(_all_logs)
            }
        )

    for manager_id in unique_managers:
        _all_logs = list(filter(lambda x: x['properties']['manager_id'] == manager_id ,log_records))
        _first = _all_logs[0]
        final_logs.append(
            {
                "CostCenterCode": "NA",
                "ManagerId": manager_id,
                "Action": _first['properties']['action'],
                "Status": get_log_status(_all_logs, status_counter),
                "Details": _first['properties']['details'],
                "JobId": _get_all_job_ids(_all_logs)
            }
        )

    set_result(key='error', val=status_counter['error'])
    set_result(key='exception', val=status_counter['exception'])
    set_result(key='success', val=status_counter['success'])

    return final_logs
