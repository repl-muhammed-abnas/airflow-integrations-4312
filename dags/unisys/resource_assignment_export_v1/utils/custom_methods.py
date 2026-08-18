import pendulum
import json
from datetime import datetime
import rail


def format_date_dd_mm_yyyy(date_string):
    if not date_string:
        return ""
    try:
        if 'T' in date_string:
            date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        else:
            date_obj = datetime.strptime(date_string, "%Y-%m-%d")

        return date_obj.strftime("%d-%m-%Y")
    except (ValueError, AttributeError):
        return date_string

def get_file_name_for_instance(config):
    prefix = config.file_prefix_map.get(config.company_key, 'Dev')
    current_date = pendulum.now(config.timezone)
    date_str = current_date.format('DDMMYYYY')

    return f"{prefix}_ResourceAssignmentsExtract_{date_str}.csv.pgp"

def get_ops_file_name_for_instance(config):
    prefix = config.file_prefix_map.get(config.company_key, 'Dev').upper()
    current_datetime = pendulum.now(config.timezone).format('YYYYMMDD_HHmmss')
    return f"{prefix}_ResourceAssignmentExtract_{current_datetime}.csv.pgp"

def get_log_file_name(config):
    prefix = "Log_" + config.file_prefix_map.get(config.company_key, 'Dev').upper()
    current_date = pendulum.now(config.timezone)
    date_str = current_date.format('MMDDYYYY')
    return f"{prefix}_ResourceAssignmentsExtract_{date_str}.txt"

def get_base_extract_file_name(config):
    prefix = config.file_prefix_map.get(config.company_key, 'Dev').upper()
    current_date = pendulum.now(config.timezone)
    date_str = current_date.format('YYYYMMDD_HHmmss')
    return f"{prefix}_ResourceAssignmentsBaseExtract_{date_str}.csv.pgp"

def prepare_csv_row_active(dag_run):
    if dag_run.conf['event_type'] == 'Deleted':        
        return {
            'Allocation Id': dag_run.conf['allocation_id'],
            'Action': 'Deleted',
            'Project Code': '',
            'Employee ID': '',
            'AllocationStartDate': '',
            'AllocationEndDate': '',
            'AllocatedHours': '',
            'latest_modified_date': dag_run.conf.get('latest_modified_date')
        }
    
    allocation_details = rail.result('get_allocation_details_graphql')
    
    return {
        'Allocation Id': dag_run.conf['allocation_id'],
        'Action': dag_run.conf['event_type'],
        'Project Code': dag_run.conf['project_code'],
        'Employee ID': dag_run.conf['employee_id'],
        'AllocationStartDate': format_date_dd_mm_yyyy(allocation_details.get('startDate', '')),
        'AllocationEndDate': format_date_dd_mm_yyyy(allocation_details.get('endDate', '')),
        'AllocatedHours': str(round(allocation_details.get('totalHours', 0), 2)) if allocation_details.get('totalHours') else '',
        'latest_modified_date': dag_run.conf.get('latest_modified_date')
    }


def extract_resource_allocations_from_graphql(response):
    allocations = response.get('data', {}).get('resourceAllocations', {}).get('resourceAllocations', [])
    if allocations:
        return allocations[0]  # Assuming we want the first allocation
    return {}

def build_log_message(config):
    logs = [
        rail.render_template(
            "File was generated at (TimeStamp is in UTC) - {{ result('get_file_generated_time') }}"
        ),
        "Webhook based integration",
        rail.render_template(
            "Total records since last run - {{ result('create_events_collection', 'length') }}"
        ),
        rail.render_template(
            "Total records exported - {{ result('gather_allocation_results') | length }}"
        ),
        "Filters used - last modified event per user per project",
        f"Export File SFTP Path - {config.sftp_remote_path}",
        rail.render_template(
            "Export File Name - {{ result('get_file_name') }}"
        ),
        f"Ops File Path - {config.sftp_ops_remote_path}",
        rail.render_template(
            "Ops File Name - {{ result('get_ops_file_name') }}"
        ),
        f"Log File Path - {config.sftp_logs_filepath}",
        rail.render_template(
            "Log File Name - {{ result('get_log_file_name') }}"
        ),
        f"Base Extract File Path - {config.sftp_logs_filepath}",
        rail.render_template(
            "Base Extract File Name - {{ result('get_base_extract_file_name') }}"
        ),
    ]

    return json.dumps([{"log": log} for log in logs])
