import pendulum
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
    current_date = pendulum.now('America/New_York')
    date_str = current_date.format('DDMMYYYY')

    return f"{prefix}_ResourceAssignmentsExtract_{date_str}.csv"

def prepare_csv_row_active(dag_run):
    if dag_run.conf['event_type'] == 'Deleted':        
        return {
            'Allocation Id': dag_run.conf['allocation_id'],
            'Action': 'Deleted',
            'Project Code': '',
            'Employee ID': '',
            'AllocationStartDate': '',
            'AllocationEndDate': '',
            'AllocatedHours': ''
        }
    
    allocation_details = rail.result('get_allocation_details_graphql')
    
    return {
        'Allocation Id': dag_run.conf['allocation_id'],
        'Action': dag_run.conf['event_type'],
        'Project Code': dag_run.conf['project_code'],
        'Employee ID': dag_run.conf['employee_id'],
        'AllocationStartDate': format_date_dd_mm_yyyy(allocation_details.get('startDate', '')),
        'AllocationEndDate': format_date_dd_mm_yyyy(allocation_details.get('endDate', '')),
        'AllocatedHours': str(round(allocation_details.get('totalHours', 0), 2)) if allocation_details.get('totalHours') else ''
    }


def extract_resource_allocations_from_graphql(response):
    allocations = response.get('data', {}).get('resourceAllocations', {}).get('resourceAllocations', [])
    if allocations:
        return allocations[0]  # Assuming we want the first allocation
    return {}
