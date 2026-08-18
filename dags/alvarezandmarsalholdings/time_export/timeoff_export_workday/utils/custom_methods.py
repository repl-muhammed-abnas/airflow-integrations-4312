import rail
from datetime import datetime


def get_timeexport_fileformat(config, response):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.timeoff_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.timeoff_export_file_format}`')


def get_timeoff_type_names_paycodes(response):
    if bool(response['rows']):
        return [{
            'timeoff_type_name': item['cells'][0]['textValue'],
            'timeoff_type_paycode':  item['cells'][1]['textValue'],
        }for item in response['rows']]
    return []


def final_export_data_callable(item):
    if not item:
        return []

    return {
        'Worker': item['employee_id'],
        'Time_Off_Entry_ID': item['short_time_entry_id'],
        'Date': item['entry_date'].replace('/', '-'),
        'Requested': item['hours'],
        'Time_Off_Type_Reference': item['timeoff_type_paycode'],
    }
