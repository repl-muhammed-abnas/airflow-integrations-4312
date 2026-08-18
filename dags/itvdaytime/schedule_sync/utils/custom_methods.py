from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta, MO, SU
import rail


REPORT_DATE_FORMAT = '%d/%m/%Y'
REPORT_FILTER_DATE_FORMAT = "%m-%d-%Y"
MAX_GUID_LENGTH = 16
SHIFT_DATE_FORMAT = f"{REPORT_DATE_FORMAT}T%H:%M:%S"
MIN_IN_SECONDS = 60


def convert_to_payload_date(date_value):
    if not date_value:
        return None
    return{
        "year": date_value.year,
        "month": date_value.month,
        "day": date_value.day
    }


def get_date(date_value, date_value_format, required_format='%Y/%m/%d'):
    if not date_value:
        return ""
    if not date_value_format:
        raise Exception("format is Required")
    if date_value_format == "json":
        return date(date_value['year'], date_value['month'], date_value['day']).strftime(required_format)
    ret_date = datetime.strptime(date_value, date_value_format)
    if required_format:
        return ret_date.strftime(required_format)
    return ret_date


def get_required_details():
    today = datetime.now()
    last_week_monday = today + relativedelta(weekday=MO(-2))
    next_third_week_sunday = today + relativedelta(weekday=SU(4))

    return{
        "today": today.strftime(REPORT_FILTER_DATE_FORMAT),
        "last_week_monday": last_week_monday.strftime(REPORT_FILTER_DATE_FORMAT),
        "next_third_week_sunday": next_third_week_sunday.strftime(REPORT_FILTER_DATE_FORMAT),
        "export_file_name": "Replicon_INT143_"+today.strftime("%Y%m%dT%H%M%S")+".csv",
        "last_week_monday_payload": convert_to_payload_date(last_week_monday),
        "next_third_week_sunday_payload": convert_to_payload_date(next_third_week_sunday),
        "timestamp": today.strftime("%Y%m%dT%H%M%S")
    }


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def convert_to_mins(data):
    if not data:
        return 0
    hour = int(data.split('.')[0])
    mins = int(data.split('.')[1])
    return (hour*60) + mins*0.6


def to_date(date_value):
    return datetime.strptime(date_value, SHIFT_DATE_FORMAT)


def get_shift_data(item):
    new_end_time = ""
    # pylint: disable=chained-comparison
    if (int(item['shift_end_time'].split(':')[0]) < 12) and (int(item['shift_start_time'].split(':')[0]) >= 12):
        new_end_time = (to_date(item['entry_date']+'T'+item['shift_end_time']
                                ) + timedelta(hours=24)).strftime(SHIFT_DATE_FORMAT)

    return {
        "shift_start_time": item['entry_date']+'T'+item['shift_start_time'],
        "shift_end_time": item['entry_date']+'T'+item['shift_end_time'],
        "break_time": item['shift_break_time'],
        "total_time": item['shift_total_hours'],
        "work_hours": item['shift_work_hours'],
        "new_shift_end_time": new_end_time if new_end_time else item['entry_date']+'T'+item['shift_end_time']
    }


def process_data_child(dag_run, raw_data_collection, data_to_process_collection):
    # using python operator as it will load all the data once into memory
    # if make use of DataAdaptor it will load the below three data for each item in
    # the memory
    data_to_process = get_data_from_document(
        rail.result(data_to_process_collection))
    all_report_data = get_data_from_document(rail.result(raw_data_collection))
    final_data = []

    # return unique_data
    def get_output_formatted_data(data, multiple):

        sum_of_break_hours_in_mins = 0
        min_start_time = ""
        max_end_time = ""
        shift_duration = 0
        if multiple:

            shifts = list(map(get_shift_data, data))
            shift_duration = sum(convert_to_mins(shift['work_hours']) for shift in shifts)
            total_break_hours_report = sum(convert_to_mins(shift['break_time']) for shift in shifts)

            min_start_time = sorted([to_date(shift['shift_start_time']) for shift in shifts])[
                0].strftime(f"{REPORT_DATE_FORMAT}T%-H:%M:%S")

            max_end_time = sorted([to_date(shift['new_shift_end_time']) for shift in shifts], reverse=True)[
                0].strftime(f"{REPORT_DATE_FORMAT}T%-H:%M:%S")

            sorted_shifts_list = sorted(
                shifts, key=lambda shift: to_date(shift['shift_start_time']))
            for i in range(len(shifts)-1):
                sum_of_break_hours_in_mins += (
                    to_date(sorted_shifts_list[i+1]['shift_start_time'])- to_date(sorted_shifts_list[i]['new_shift_end_time'])).seconds/MIN_IN_SECONDS

            sum_of_break_hours_in_mins += total_break_hours_report
        # whichever the 1st shift of day is will be the master data
        data = [data[0]] if not multiple else [list(filter(
            lambda i: i['shift_start_time'] == min_start_time.split('T')[-1], data))[0]]

        return list(map(lambda item: {
            'resource_reference_type': item['assignment_id'] if item['assignment_id'] else item['employee_id'],
            'period_start_date': get_date(dag_run.conf['get_required_details']['last_week_monday'], REPORT_FILTER_DATE_FORMAT),
            'period_end_date': get_date(dag_run.conf['get_required_details']['next_third_week_sunday'], REPORT_FILTER_DATE_FORMAT),
            'publish': "Y",  # hardcoded to 'Y'
            'shift_number': str((str(item['useruri'].split(':')[-1]) + get_date(item["entry_date"], REPORT_DATE_FORMAT, "%d%m%Y")).rjust(MAX_GUID_LENGTH, '0')),
            'shift_actions': "",  # hardcoded to blank
            'reference_day': get_date(item["entry_date"], REPORT_DATE_FORMAT),

            'shift_start_time': item["shift_start_time"] if not multiple else min_start_time.split("T")[-1],
            'shift_end_time': item["shift_end_time"] if not multiple else max_end_time.split("T")[-1],
            'shift_duration': convert_to_mins(item['shift_work_hours']) if not multiple else shift_duration,
            'shift_time_not_worked': convert_to_mins(item['shift_break_time']) if not multiple else sum_of_break_hours_in_mins,
            'shift_code': item['shift_code'],
            'shift_category': item['shift_name'],

            'shift_type': item["shift_type"],
            'allow_shift': "Y"  # hardcoded to 'Y'
        } if item['useruri'] else None, data))

    def get_all_shifts_assigned_for_same_date(useruri, entry_date):
        return list(filter(lambda x: (get_date(x['entry_date'], REPORT_DATE_FORMAT) == get_date(
                    entry_date, REPORT_DATE_FORMAT)) and x['useruri'] == useruri and x['useruri'], all_report_data))

    for record in data_to_process:
        useruri, entry_date = record['useruri'], record['entry_date']
        all_shifts_for_same_date = get_all_shifts_assigned_for_same_date(
            useruri, entry_date)

        # users who has only 1 shift on same day
        if len(all_shifts_for_same_date) == 1:
            final_data.extend(get_output_formatted_data(
                data=all_shifts_for_same_date, multiple=False))
            continue

        # for user who has multiple shifts on the same day
        if len(all_shifts_for_same_date) > 1:
            final_data.extend(get_output_formatted_data(
                data=all_shifts_for_same_date, multiple=True))

    # main return
    return list(filter(None, final_data))
