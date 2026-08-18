from datetime import date, datetime, timedelta
import calendar
import hashlib
from pandas.tseries.offsets import BMonthEnd
import rail
from dxctechnology.australia_payroll_extract_v2.utils import request_payload

today=date.today()
def check_schedule():
    return date.today() == BMonthEnd().rollforward(today).strftime("%Y-%m-%d")

def check_week_day():
    return today.weekday().weekday() !=4

def load_records(task_id):
    return rail.load_all_records(rail.result(task_id))


def check_schedule_for_user_schedule():
    today_date= today.strftime("%d")
    current_date= datetime.now()
    last_day_of_the_month= calendar.monthrange(current_date.year,current_date.month)[1]
    five_days_before_end_of_month= (datetime(current_date.year,current_date.month, last_day_of_the_month) - timedelta(days=5)).strftime("%d")

    return today_date in ('03', '08', five_days_before_end_of_month)


def get_dates_format(received_date, received_format):
    strip_time= datetime.strptime(received_date, received_format)
    strif_time= datetime.strftime(strip_time, "%Y-%m-%d")
    return strif_time

def get_converted_query_data(item):
    get_filter_start_date= get_dates_format(request_payload.get_dates()['start_date'], '%m/%d/%Y')
    if not item:
        return []

    return {
        'Employee_Id': item['Actual_Employee_ID'] if item['Actual_Employee_ID'] else item['Employee_Id'],
        'Shift_Description': item['Shift_Description'] if item['Shift_Description'] else None,
        'Office_Schedule': item['Office_Schedule'] if item['Office_Schedule'] else None,
        'Actual_Employee_ID': item['Actual_Employee_ID'] if item['Actual_Employee_ID'] else None,
        'Schedule_Name': item['Schedule_Name'] if item['Schedule_Name'] else None,
        'start_date': get_filter_start_date,
        'end_date': '99991231'
    }

def get_signgle_schedule_dates():
    query_data= load_records("query_distinct_schedules_for_employee_id")
    get_filter_start_date= get_dates_format(request_payload.get_dates()['start_date'], '%m/%d/%Y')
    description = query_data[0]['Shift_Description'] if query_data[0]['Shift_Description'] else (query_data[0][
        'Schedule_Name'] if query_data[0]['Schedule_Name'] else '')
    employeeid = query_data[0]['Actual_Employee_ID'] if query_data[0]['Actual_Employee_ID'] else query_data[0]['Employee_Id']
    return{
        'Employee_Id': employeeid,
        'Description': description,
        'Start_date': get_filter_start_date,
        'End_date': '99991231',
        'md5': hashlib.md5((str(employeeid) + ',' + str(description)).encode('utf-8')).hexdigest()
    }

def get_multiple_schedule_dates_withmd5():
    def get_task_state(task_id):
        return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

    query_data= load_records("query_final_dates") if get_task_state(
        "create_final_dates_collection") == "success" else load_records("get_data_for_single_schedule")
    return list(map(lambda item: {
        'Employee_Id': item['Employee_Id'],
        'Description': item['Shift_Description'] if item['Shift_Description'] else item['Schedule_Name'],
        'Start_date': item['start_date'],
        'End_date': item['end_date'],
        'md5': hashlib.md5((str(item['Employee_Id']) + ',' + str(item['Shift_Description']) + ',' + str(item['Schedule_Name'])).encode('utf-8')).hexdigest()
    },query_data))

def get_multiple_schedule_dates():
    max_data_count = rail.result('query_data_for_employee_id', 'length')
    result = []
    data = load_records('query_data_for_employee_id')
    previous_shift = []
    shift_dates = {"start_date": "", "end_date":""}
    def check_if_next_shift_is_same(shift_name, idx) -> bool:
        if not data[idx+1]['Shift_Description']:
            return True
        return data[idx+1]['Shift_Description'] == shift_name

    def update_the_start_end_date_for_all_pervious_shifts():
        for idx in previous_shift:
            result[idx]['start_date'] = shift_dates['start_date']
            result[idx]['end_date'] = shift_dates['end_date']

    def reset_previous_shift():
        update_the_start_end_date_for_all_pervious_shifts()
        previous_shift.clear()
        shift_dates['start_date'] = None
        shift_dates['end_date'] = None

    def add_to_result(item):
        result.append(item)

    for idx, item in enumerate(data):
        add_to_result(item)
        if idx+1 < max_data_count:
            if not previous_shift:
                shift_dates['start_date'] = item['Entry_Date']

            previous_shift.append(idx)
            shift_dates['end_date'] =(datetime.strptime(data[idx+1]['Entry_Date'], "%Y-%m-%d") - timedelta(days=1)).strftime('%Y-%m-%d')

            _check_if_next_shift_is_same = check_if_next_shift_is_same(item['Shift_Description'], idx)

            if not _check_if_next_shift_is_same:
                reset_previous_shift()
            continue

        check_if_previous_shift_is_same = bool(previous_shift)
        previous_shift.append(idx)

        if not check_if_previous_shift_is_same:
            shift_dates['start_date'] = item['Entry_Date']
        shift_dates['end_date'] = '99991231'
        reset_previous_shift()

    return result

def has_any_file(result_task_id, input_file_path):
    if not result_task_id or not input_file_path:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")
    data = rail.result(result_task_id)
    if not data:
        return False
    return len(data[input_file_path]) > 0

def get_create_existing_md5(item):
    if not item:
        return []
    res = {
        "Employee_Id": item['Employee_Id'],
        "Actual_Employee_ID": item['Actual_Employee_ID'],
        "Description": item['Description'],
        "Start_date": item['Start_date'],
        "End_date": item['End_date'],
        "md5": hashlib.md5((str(item['Employee_Id']) + "," + str(item['Description']) + "," + str(item['Actual_Employee_ID']) +
                            str(item['Start_date']) + "," + str(item['End_date'])).encode('utf-8')).hexdigest()
    }
    return {k: v if v is not None else '' for k, v in res.items()}


def check_2010_infotype():
    data = rail.result("get_es_holiday_calander_for_daterange")[0]['name']
    if "2010" in data:
        if "semi-monthly" in data.lower() and "standard" in data.lower():
            return {
                'date': '03',
                'condition': True
            }
        if "semi-monthly" in data.lower() and "standard" not in data.lower():
            return {
                'date': '23',
                'condition': True
            }
    return {
        'date': None,
        'condition': False
    }

def check_0015_infotype():
    return "0015" in rail.result("get_es_holiday_calander_for_daterange")[0]['name']

def check_0007_infotype():
    return "0007" in rail.result("get_es_holiday_calander_for_daterange")[0]['name']

def check_2006_infotype():
    return "2006" in rail.result("get_es_holiday_calander_for_daterange")[0]['name']
