from json import dumps
from dateutil.parser import parse as date_parser
import pendulum
import rail


EXPORT_DATE_FORMAT = "%Y-%m-%d"
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
        "time_export_name": f"Time Extract_{offset_time}",
        "no_data_time_export_name": f"NO_DATA_Time_Extract_{offset_time}",
        "no_valid_data_time_export_name": f"NO_VALID_DATA_Time_Extract_{offset_time}"
    }


def convert_to_24hours_format(time_string: str, caller = "none") -> str:
    # only applicable for the out time
    if caller == "out_time":
        if time_string == "11:59:59 PM" or time_string == "23:59:59" or time_string == "11:59:00 PM" or time_string == "23:59:00":
            return date_parser("12:00:00 AM").strftime(EXPORT_TIME_FORMAT)
    return date_parser(time_string).strftime(EXPORT_TIME_FORMAT)


def convert_date_to_export_formate(date_string: str) -> str:
    return date_parser(date_string).strftime(EXPORT_DATE_FORMAT)


def format_numeric_value(value):
    if not value or str(value).strip().lower() == "null":
        return value
    str_value = str(value).strip()
    if ',' in str_value and any(c.isdigit() for c in str_value):
        return str_value.replace(",", "")
    return str_value


def format_raw_export_data_callable(item):
    if not item:
        return []

    return {
        'record_id': item['record_id'],
        'sap_counter_id': item['sap_counter_id'],
        'entry_date': convert_date_to_export_formate(item['entry_date']),
        'user': item['user'],
        'employee_id': item['employee_id'],
        'activity_name': item['activity_name'],
        'activity_code': item['activity_code'],
        'project_name': item['project_name'],
        'project_code': item['project_code'],
        'task_name': item['task_name'],
        'task_code': item['task_code'],
        'in_time': convert_to_24hours_format(item['in_time'], "in_time"),
        'out_time': convert_to_24hours_format(item['out_time'], "out_time"),
        'short_time_entry_id': item['short_time_entry_id'],
        'source_system': SOURCE_SYSTEM,
        'crane_capacity': item['crane_capacity'],
        'hours': item['hours'],
        'time_entry_type': item['time_entry_type'],
        'account_indicator': item['account_indicator'],
        'time_entry_id': item['time_entry_id']
    }


def final_export_data_callable(item):
    if not item:
        return []

    return {
        'SAP_Counter_ID': item['sap_counter_id'],
        'Entry_Date': convert_date_to_export_formate(item['entry_date']),
        'Employee_ID': item['employee_id'],
        'Activity_Type': item['activity_code'],
        'Project_Code': item['project_code'],
        'Task_ID': item['task_name'],
        'In_Time': convert_to_24hours_format(item['in_time']),
        'Out_Time': convert_to_24hours_format(item['out_time']),
        'Entry_ID': item['short_time_entry_id'], # short time entry id
        'Source_system': SOURCE_SYSTEM,
        'Crane_capacity': format_numeric_value(item['crane_capacity']),
        'Hours': item['hours'],
        'Time_Entry_Type': item['time_entry_type'],
        'Account_Indicator': item['account_indicator'],
        'Replicon_Unique_ID': item['time_entry_id']
    }


def create_json_payload_callable(task_id):
    return dumps({
        "TimeData": rail.load_all_records(rail.result(task_id))
    })
