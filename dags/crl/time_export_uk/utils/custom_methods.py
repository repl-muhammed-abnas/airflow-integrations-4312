from json import dumps
from dateutil.parser import parse as date_parser
import pendulum
import rail


EXPORT_DATE_FORMAT = "%m/%d/%Y"
EXPORT_FILE_TIMESTAMP = "%Y%m%d%H%M%S"
SOURCE_SYSTEM = "Replicon"
EXPORT_TIME_FORMAT = '%H:%M:%S'

null = None


def get_timeexport_fileformat(config, response):
    """
    Get the time export file format URI from the response
    """
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.time_export_file_format}`')


def get_time_export_name(config):
    """
    Generate time export file names based on current timestamp
    """
    offset_time = pendulum.now(config.time_zone).strftime(EXPORT_FILE_TIMESTAMP)
    return {
        "time_export_name": f"Time Extract_UK_{offset_time}",
        "no_data_time_export_name": f"NO_DATA_Time_Extract_UK_{offset_time}"
    }


def convert_date_to_export_formate(date_string: str) -> str:
    """
    Convert date string to MM/DD/YYYY format
    """
    return date_parser(date_string).strftime(EXPORT_DATE_FORMAT)


def format_raw_export_data_callable(item, config):
    """
    UK-specific logic for formatting export data
    
    Pay Type Priority:
    1. Time off description (for time off bookings)
    2. Time Type (UK) Code via mapper lookup
    3. Default to '0' (as per spec)
    4. Special case: Holiday override to '0113'
    
    Activity Logic:
    - If project selected and activity selected: use activity name
    - If project selected but no activity: use default activity
    - Otherwise: blank
    """
    if not item:
        return []

    # Pay Type Logic
    pay_type = ''
    
    # Priority 1: Time off description (for time off bookings)
    if item.get('timeoff_type_description'):
        pay_type = item['timeoff_type_description']
    
    # Priority 2: Time Type (UK) Code via mapper lookup
    elif item.get('time_type_uk_code'):
        pay_type = rail.find_first_by_attr_and_get_attr(
            config.pay_code_mapper, 
            'time_type_code', 
            item['time_type_uk_code'], 
            'sap_time_code'
        )
    
    # Priority 3: Default to '0800' (as per spec)
    if not pay_type:
        pay_type = '0800'
    
    # Priority 4: Special case - Holiday override
    if item.get('timeoff_type_name') == 'Holiday':
        pay_type = '0113'
    
    # Activity Logic (simplified from US PAI logic)
    activity_name = ''
    if item.get('network_code'):  # If project selected
        if item.get('activity_name'):  # If activity selected
            activity_name = item['activity_name']
        elif item.get('default_activity'):  # If no activity but has default
            activity_name = item['default_activity']
    # Otherwise remains blank

    return {
        'employee_id': item['employee_id'],
        'entry_date': convert_date_to_export_formate(item['entry_date']),
        'hours': item['hours'],
        'pay_type': pay_type,
        'activity_name': activity_name,
        'punch_in_date': convert_date_to_export_formate(item['punch_in_date']) if item['punch_in_date'] 
                        else convert_date_to_export_formate(item['entry_date']),
        'punch_out_date': convert_date_to_export_formate(item['punch_out_date']) if item['punch_out_date'] 
                         else convert_date_to_export_formate(item['entry_date']),
        'project_code': item['project_code'],
        'network_code': item['network_code'],
        'task_code': item['task_code'],
        'short_id': item['short_id'],  # UK always uses short_id (no punch_entry_id fallback needed)
        'transaction_type': 'N'
    }


def final_export_data_callable(item):
    """
    Convert formatted data to final export format with string types
    """
    if not item:
        return []

    return {
        'Employee_Number': str(item['employee_id']),
        'Event_Date': str(item['entry_date']),
        'Allocated_Hours': str(item['hours']),
        'Pay_Type': str(item['pay_type']),
        'Activity_Type': str(item['activity_name']),
        'Actual_Start_Date': str(item['punch_in_date']),
        'Actual_End_Date': str(item['punch_out_date']),
        'Project_ID': str(item['project_code']),
        'Network_ID': str(item['network_code']),
        'Network_Activity': str(item['task_code']),
        'spanid': str(item['short_id']),
        'Transaction_Type': item['transaction_type']
    }


def create_json_payload_callable(task_id):
    """
    Create JSON payload with postings array
    """
    return dumps({
        "postings": rail.load_all_records(rail.result(task_id))
    })


def get_filtered_allowed_location_uris(response, export_locations):
    """
    Filter location URIs to only include locations matching export_locations config
    """
    if not response.get('rows'):
        return []

    location_list = list(filter(
        lambda item: item['name'] == export_locations,
        map(lambda data: {
            'name': data['cells'][1]['cellCollection'][0]['textValue'],
            'uri': data['cells'][0]['uri']
        }, response['rows'])
    ))

    return [item['uri'] for item in location_list]