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
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.time_export_file_format}`')


def get_time_export_name(config):
    offset_time = pendulum.now(config.time_zone).strftime(EXPORT_FILE_TIMESTAMP)
    return {
        "time_export_name": f"Time Extract_Germany_{offset_time}",
        "no_data_time_export_name": f"NO_DATA_Time_Extract_Germany_{offset_time}"
    }


def convert_date_to_export_formate(date_string: str) -> str:
    return date_parser(date_string).strftime(EXPORT_DATE_FORMAT)


def format_raw_export_data_callable(item, config):
    """
    Germany-specific pay type logic:

    1. Time off bookings  → pay_type from timeoff_type_description (SAP code stored in Replicon)
    2. Time Type (Germany) code → lookup in pay_code_mapper
    3. Default → '0800' (Regular Hours, per spec section 6.0)

    Activity Logic:
    - Project selected + activity selected  → use activity name
    - Project selected + no activity        → use default activity
    - Otherwise                             → blank
    """
    if not item:
        return []

    pay_type = ''

    if item.get('timeoff_type_description'):
        pay_type = item['timeoff_type_description']

    elif item.get('time_type_germany_code'):
        pay_type = rail.find_first_by_attr_and_get_attr(
            config.pay_code_mapper,
            'time_type_code',
            item['time_type_germany_code'],
            'sap_time_code'
        )

    if not pay_type:
        pay_type = '0800'

    activity_name = ''
    if item.get('network_code'):
        if item.get('activity_name'):
            activity_name = item['activity_name']
        elif item.get('default_activity'):
            activity_name = item['default_activity']

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
        'short_id': item['short_id'] if item['short_id'] else item['punch_entry_id'],
        'transaction_type': 'N'
    }


def final_export_data_callable(item):
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
    return dumps({
        "postings": rail.load_all_records(rail.result(task_id))
    })


def get_filtered_allowed_location_uris(response, export_locations):
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
