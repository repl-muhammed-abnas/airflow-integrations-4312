# pylint: disable=no-else-return, line-too-long, inconsistent-return-statements, too-many-return-statements
from datetime import timedelta, datetime
import hashlib
import rail


def create_timeofftypes_list(response):
    timeoff_list = []
    for item in response:
        timeoff_list.append({"timeoffname": item['cells'][0].get('textValue'),
                             "timeoffdescription": item['cells'][1].get('textValue'),
                             "status": item['cells'][2].get('textValue'),
                             "timeoffuri": item['cells'][0].get('uri')}),
    return timeoff_list


def check_for_weekday(dag_run):
    if dag_run.conf["Startdate"]:
        if datetime.strptime(dag_run.conf["Startdate"],  '%m/%d/%Y').weekday() not in [5, 6]:
            return {"status": True, "message": ""}
        else:
            return {"status": False, "message": "Start Date is Saturday or Sunday"}
    else:
        return {"status": False, "message": "Start Date is not present"}


def required_parameter(dag_run):
    if not dag_run.conf["Startdate"]:
        return {"status": False, "message": "Start Date is not present"}
    elif not dag_run.conf["Timeoffhrs"]:
        return {"status": False, "message": "Time Off hours are not present"}
    elif not dag_run.conf["Startdate"] == dag_run.conf["Enddate"]:
        return {"status": False, "message": "Start and End date are not same"}
    elif not dag_run.conf["Uniqueid"]:
        return {"status": False, "message": "Unique ID is not present"}
    elif not dag_run.conf["Useruri"]:
        return {"status": False, "message": "User is not present or disabled"}
    elif not dag_run.conf["Timeoffuri"]:
        return {"status": False, "message": "Timeoff Type is not present"}
    elif datetime.strptime(dag_run.conf["User start date"], "%b %d, %Y") > datetime(dag_run.conf["Bookingdate"]['year'], dag_run.conf["Bookingdate"]['month'], dag_run.conf["Bookingdate"]['day']) or (len(dag_run.conf["User end date"]) >= 10 and datetime.strptime(dag_run.conf["User end date"], "%b %d, %Y") < datetime(dag_run.conf["Bookingdate"]['year'], dag_run.conf["Bookingdate"]['month'], dag_run.conf["Bookingdate"]['day'])):
        return {"status": False, "message":  "Booking date is outside of user start date and end date"}
    elif not int(dag_run.conf["Timeoffhrs"]) > 0 and dag_run.conf["Action"] != "DELETE":
        return {"status": False, "message": "Time Off hours should be greater than 0"}
    else:
        return {"status": True, "message": ""}


def check_startdate_eligible(dag_run):
    if datetime.strptime(dag_run.conf["Startdate"], '%m/%d/%Y') < (datetime.today() + timedelta(days=1)):
        return True
    else:
        False


def check_for_timesheet_uri(dag_run):
    if [timeoff for timeoff in rail.result('get_time_off_type_assignments_for_user') if timeoff['uri'] == dag_run.conf['Timeoffuri']]:
        return True
    else:
        False


def get_totalhours_list():
    if rail.result('get_time_off_details_for_user_and_date_range') and rail.result('get_time_off_details_for_user_and_date_range'):
        return [timeoff.get('totalDuration').get('calendarDayDuration').get('hours') for timeoff in rail.result('get_time_off_details_for_user_and_date_range')]
    else:
        return []


def get_sum_of_total_timeoff_hours(dag_run):
    return (sum(rail.result('get_timeoff_hours_list')) == 8) or (sum(rail.result('get_timeoff_hours_list')) > 8) or ((sum(rail.result('get_timeoff_hours_list')) + int(dag_run.conf["Timeoffhrs"])) > 8)


def create_UID(item):
    return [
        item['EmployeeID'],
        item['Timeofftype'],
        item['StarDate'],
        item['EndDate'],
        item['Hrs'],
        item['Action'],
        item['UniqueID'],
        hashlib.md5((item['EmployeeID'] + ',' + item['Timeofftype'] + ',' + item['StarDate'] + ',' + item['EndDate'] +
                    ',' + item['Hrs'] + ',' + item['Action'] + ',' + item['UniqueID']).encode('utf-8')).hexdigest()
    ]


def check_for_failure():
    if len(rail.result('gather_child_error_data')) > 0:
        if any((True for response in rail.result('gather_child_error_data') if response.startswith("artifact"))):
            return True
    return False
