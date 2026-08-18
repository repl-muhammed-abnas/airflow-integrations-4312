from datetime import datetime, timedelta
import rail

def get_assigned_shift_dates(shift_name):
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
            'todelete': "No" if scheduled_shift_name == shift_name else "Yes"
        }
        shift_list.append(shift)
    return shift_list

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_start_date():
    dag_conf = get_dag_run_conf()
    return dag_conf['Startdate']

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

def get_shift_assignment_list(shift_name, useruri):
    shift_assignments_data = rail.load_all_records(rail.result(
    "query_shift_assignment"))
    shift_assignments = []
    for each_data in shift_assignments_data:

        each_shift = [
            {
            "date": {
                "year": each_data['dateyear'],
                "month": each_data['datemonth'],
                "day": each_data['dateday']
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
        shift_assignments.append(each_shift)
    shift_assignments_list = [data for shifts in shift_assignments for data in shifts]
    return shift_assignments_list

def check_any_shifts_to_be_deleted():
    shift_assignments_data = rail.load_all_records(rail.result(
    "get_assigned_shift_dates"))
    return bool(list(filter(lambda shift_data: shift_data['todelete'] == 'Yes', shift_assignments_data)))
