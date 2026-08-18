from datetime import datetime, timedelta
import rail

def check_has_user_to_process():
    users_to_process = rail.result(
        "get_shift_user_in_reference_file")
    return bool(users_to_process)

def get_number_of_days_till_friday(start_date):
    date_mapper = {
        'Sunday' : 0,
        'Monday' : 5,
        'Tuesday' : 4,
        'Wednesday' : 3,
        'Thursday' : 2,
        'Friday' : 1,
        'Saturday' : 0
    }
    number_of_days_present_till_friday = date_mapper.get(datetime.strptime(start_date, "%Y-%m-%d").strftime("%A"), 0)
    return number_of_days_present_till_friday

def get_day_and_week_number_of_year(start_date):
    days_mapper = {
        'Sunday' : 6,
        'Monday' : 5,
        'Tuesday' : 4,
        'Wednesday' : 3,
        'Thursday' : 2,
        'Friday' : 1,
        'Saturday' : 7
    }
    # pylint: disable=line-too-long
    friday_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=int(days_mapper.get(datetime.strptime(start_date, "%Y-%m-%d").strftime("%A"), 0))))
    day_number = int(friday_date.strftime("%j"))
    week_number = friday_date.isocalendar()[1]
    return day_number, week_number, friday_date

def create_dates_till_friday(start_date):
    day_number, week_number, friday_date = get_day_and_week_number_of_year(start_date)
    weekday = (datetime.strptime(start_date, "%Y-%m-%d")).strftime("%A")
    dates = {
        'today' : start_date,
        'todaysweekday' : weekday,
        'daystillfriday' : get_number_of_days_till_friday(start_date),
        'yday' : day_number,
        'week' : week_number,
        'friday' : friday_date.strftime("%Y-%m-%d")
    }
    return dates

def add_each_shift_data(shift_data, shift_type, useruri, shift_name=None):
    if shift_name is None:
        shift_name = "VDC System Shift" if shift_type == "VDC" else "System Shift"
    each_shift = [
            {
            "date": {
                "year": shift_data['dateyear'],
                "month": shift_data['datemonth'],
                "day": shift_data['dateday']
            },
            "target": {
                "uri": None
            },
            "shift": {
                "uri": None,
                "name": shift_name
            },
            "user": {
                "uri": useruri,
                "loginName": None,
                "parameterCorrelationId": None
            },
            "note": "Published by shift automation",
            "publishState": "urn:replicon:shift-assignment-publish-state:published"
        }
    ]
    return each_shift

def add_shift_assignments_for_next_week(shift_type, useruri, retrive_task, shift_name=None):
    all_shift_assignments_for_next_week = []
    shift_assignment_for_next_week = rail.load_all_records(rail.result(
    retrive_task))
    for each_data in shift_assignment_for_next_week:
        each_shift = each_shift = add_each_shift_data(each_data, shift_type, useruri, shift_name)
        all_shift_assignments_for_next_week.append(each_shift)
    return all_shift_assignments_for_next_week

def get_final_shift_assignment_list():
    shift_assignment_list_till_friday = rail.result(
        "add_shift_assignments_till_friday") or []
    shift_assignment_list_for_next_week = rail.result(
        "add_shift_assignment_for_next_week") or []
    shift_assignment_list_for_week = rail.result(
        "add_shift_assignment_for_week") or []
    combined_shift_list = [*shift_assignment_list_till_friday, *shift_assignment_list_for_next_week, *shift_assignment_list_for_week]
    final_shift_Assignment_list = [shift_assignment for shift_list in combined_shift_list for shift_assignment in shift_list]
    return final_shift_Assignment_list

def get_assigned_shift_dates():
    shift_details = rail.load_all_records(rail.result(
        "get_shift_schedule_summary"))
    shift_list = []

    for shift_detail in shift_details:
        date = datetime(shift_detail['date']['year'], shift_detail['date']['month'],  shift_detail['date']['day'])
        str_date = date.strftime("%Y-%m-%d")
        week_number = date.isocalendar()[1]+1 if date.weekday == 0 else date.isocalendar()[1]
        scheduled_shift_name = shift_detail['shift']['displayText']
        shift = {
            'date':str_date,
            'week': week_number,
            'shift': scheduled_shift_name,
            'assignmenturi': shift_detail['assignmentUri'],
        }
        shift_list.append(shift)
    return shift_list

def create_date_range_seq(end_date_str, start_date_str):
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    days_difference = (end_date - start_date).days
    days_seq_list = []
    for day_index in range(1, days_difference+1):
        day_seq_dict = {
            'seq': day_index
        }
        days_seq_list.append(day_seq_dict)

    return days_seq_list
