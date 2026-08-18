from datetime import date, datetime, timedelta
import calendar
import hashlib
from pandas.tseries.offsets import BMonthEnd
import rail
from dxctechnology.australia_payroll_extract_v1.utils import request_payload

today=date.today()
offset= BMonthEnd()

last_day= offset.rollforward(today).strftime("%Y-%m-%d")
week_day= today.weekday()

def check_schedule():
    return bool(today == last_day)

def check_week_day():
    return bool(week_day !=4)


def check_schedule_for_user_schedule():
    today_date= today.strftime("%d")
    current_date= datetime.now()
    last_day_of_the_month= calendar.monthrange(current_date.year,current_date.month)[1]
    five_days_before_end_of_month= (datetime(current_date.year,current_date.month, last_day_of_the_month) - timedelta(days=5)).strftime("%d")

    return bool(today_date in ('03', '08', five_days_before_end_of_month))


def get_dates_format(received_date, received_format):
    strip_time= datetime.strptime(received_date, received_format)
    strif_time= datetime.strftime(strip_time, "%Y-%m-%d")
    return strif_time

def calculate_dates():
    data= rail.load_all_records(rail.result("query_for_dates"))
    get_filter_start_date= get_dates_format(request_payload.get_dates()['start_date'], '%m/%d/%Y')
    get_filter_end_date= get_dates_format(request_payload.get_dates()['end_date'], '%m/%d/%Y')

    derived_start_date= get_dates_format(data[0]['MIN_Entry_Date_'], '%Y-%m-%d')
    derived_end_date= get_dates_format(data[0]['MAX_Entry_Date_'], '%Y-%m-%d')

    if data[0]['Shift_Description'] or data[0]['Office_Schedule'] == 'Shift Schedule':
        if derived_start_date == get_filter_start_date:
            start_date= get_filter_start_date
        else:
            start_date = derived_start_date
        if derived_end_date == get_filter_end_date:
            end_date= '99991231'
        else:
            user_end_date= rail.load_all_records(rail.result("get_max_end_date"))[0]['MIN_Entry_Date_']
            if user_end_date:
                end_date= (datetime.strptime(user_end_date, "%Y-%m-%d") - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                end_date = '99991231'
        return{
            'start_date': start_date,
            'end_date': end_date
        }
    if data[0]['Schedule_Name'] or data[0]['Office_Schedule']:
        if derived_start_date == get_filter_start_date:
            start_date= get_filter_start_date
        else:
            start_date = derived_start_date
        if derived_end_date == get_filter_end_date:
            end_date= '99991231'
        else:
            end_date = derived_end_date
        return{
            'start_date': start_date,
            'end_date': end_date
        }
    return{
            'start_date': None,
            'end_date': None
        }

def get_required_data():
    query_data= rail.load_all_records(rail.result("query_for_dates"))
    dates= rail.result("calculate_start_date_and_end_date")
    description = query_data[0]['Shift_Description'] if query_data[0]['Shift_Description'] else (query_data[0][
        'Schedule_Name'] if query_data[0]['Schedule_Name'] else '')
    employeeid = query_data[0]['Employee_Id'] if query_data[0]['Employee_Id'] else query_data[0]['Actual_Employee_ID']
    return{
        'Employee_Id': employeeid,
        'Description': description,
        'Start_date': dates['start_date'],
        'End_date': dates['end_date'],
        'md5': hashlib.md5((str(employeeid) + ',' + str(description)).encode('utf-8')).hexdigest()
    }

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

def get_converted_query_data(item):
    if not item:
        return []

    return {
        'Entry_Date': item['Entry_Date'] if item['Entry_Date'] else None,
        'Shift_Description': item['Shift_Description'] if item['Shift_Description'] else None,
        'Schedule_Name': item['Schedule_Name'] if item['Schedule_Name'] else None,
        'Employee_Id': item['Employee_Id'] if item['Employee_Id'] else None,
        'Actual_Employee_ID': item['Actual_Employee_ID'] if item['Actual_Employee_ID'] else None,
        'Office_Schedule': item['Office_Schedule'] if item['Office_Schedule'] else None,
    }
