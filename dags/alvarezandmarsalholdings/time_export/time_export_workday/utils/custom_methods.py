import rail
from datetime import datetime


def get_timeexport_fileformat(config, response):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.time_export_file_format}`')


def get_office_location_details(response):
    if bool(response['rows']):
        return [{
            'office_location_name': item['cells'][0]['textValue'],
            'office_location_fullpath': "/".join([x['textValue'] for x in item['cells'][1]['cellCollection']]),
            'office_location_level_2': item['cells'][1]['cellCollection'][1]['textValue'] if len(
                item['cells'][1]['cellCollection']) > 1 else item['cells'][1]['cellCollection'][0]['textValue']
        }for item in response['rows']]

    return []


def get_time_entry_code_reference_mapper_result(mapper, item):
    if item['break_type_name'] == 'Meal':
        return 'MEAL'
    office_location_level_2 = item['office_location_fullpath'].split(
        '/')[1].strip() if item['office_location_fullpath'] and "/" in item['office_location_fullpath']  else ''
    mapper_value = next(iter(filter(lambda x: x['entry_type'] == 'Work Hours' and (
        x['pay_rate_type'] == item['pay_rate_name']) and x['job_exempt'] == item['job_exempt_name'] and (
            x['office_country'] == office_location_level_2), mapper)), {}).get('value', 'Salary Hours')
    return mapper_value


def convert_to_24hours_format(entry_date, time_12_hours):
    time_24_hours = datetime.strptime(
        time_12_hours, "%I:%M:%S %p").strftime("%H:%M:%S")
    return f"{entry_date.replace('/', '-')}T{time_24_hours}"


def final_export_data_callable(time_entry_code_ref_mapper, item):
    if not item:
        return []

    time_entry_reference_code_value = get_time_entry_code_reference_mapper_result(
        time_entry_code_ref_mapper, item)

    return {
        'Worker_Reference': item['employee_id'],
        'Worker_Time_Block_Reference': item['short_time_entry_id'],
        'Delete_Time_Block': "Y" if item['hours_current'] == '0.00' else '',
        'Time_Entry_Code_Reference': time_entry_reference_code_value,
        'Date': item['entry_date'].replace('/', '-'),
        'Quantity': item['hours_current'],
        'In_Date_Time': convert_to_24hours_format(item['entry_date'], item['in_time']) if item['in_time'] else '',
        'Out_Date_Time': convert_to_24hours_format(item['entry_date'], item['out_time']) if item['out_time'] else '',
        'Out_Reason_Reference': "" if time_entry_reference_code_value == "MEAL" else "OUT",
        'Comment': item['project_code'],
    }
