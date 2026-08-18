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
    offset_time = pendulum.now(
        config.time_zone).strftime(EXPORT_FILE_TIMESTAMP)
    return {
        "time_export_name": f"Time Extract_PTA_US_{offset_time}",
        "no_data_time_export_name": f"NO_DATA_Time_Extract_PTA_US_{offset_time}"
    }


def convert_date_to_export_formate(date_string: str) -> str:
    return date_parser(date_string).strftime(EXPORT_DATE_FORMAT)

def format_raw_export_data_callable(item,config):

    if not item:
        return []

    time_type = list(filter(lambda data: data['pay_code'] == item['distributed_time_type_code'], config.pay_code_mapper))
    pay_type = ''

    if item['employee_type'] in config.non_project_employee_types or item['employee_type'] in config.project_employee_types:
        pay_type = '0800'

    elif time_type:
        pay_type = time_type[0]['sap_time_code']

    return {
        'employee_id': item['employee_id'],
        'entry_date': convert_date_to_export_formate(item['entry_date']),
        'hours': item['hours'],
        'pay_type': pay_type if pay_type else '0860' if (item['timeoff_type_name'] == 'Holiday') else item[
            'timeoff_type_description'] if item['timeoff_type_description'] else item[
            'time_type_usa_code'] if item['time_type_usa_code'] else item['time_type_na04_code'] if item[
                'time_type_na04_code'] else '0800',
        'activity_name': item['activity_name'] if item['network_code'] and item['activity_name'] else item[
            'default_activity'] if item['network_code'] and not item['activity_name'] and item[
                'location_name'] in config.pai_locations else '',
        'punch_in_date': convert_date_to_export_formate(item['punch_in_date']) if item[
            'punch_in_date'] else convert_date_to_export_formate(item['entry_date']),
        'punch_out_date': convert_date_to_export_formate(item['punch_out_date']) if item[
            'punch_out_date'] else convert_date_to_export_formate(item['entry_date']),
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

def get_filtered_allowed_location_uris(response):
    if not response['rows']:
        return []

    location_list = list(filter(lambda item: item['name'] == 'USA', map(lambda data:{
        'name': data['cells'][1]['cellCollection'][0]['textValue'],
        'uri': data['cells'][0]['uri']
    },response['rows'])))

    return [item['uri'] for item in location_list]
