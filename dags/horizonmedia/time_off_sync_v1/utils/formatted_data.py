# pylint: disable=no-else-return, line-too-long, inconsistent-return-statements, too-many-return-statements
from datetime import timedelta, datetime
import hashlib
import rail

null = None

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
    elif not int(dag_run.conf["Timeoffhrs"]) > 0 and dag_run.conf["Action"] not in ["DELETE", "UPDATE"]:
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
    return (sum(rail.result('get_timeoff_hours_list')) == 8) or (sum(rail.result('get_timeoff_hours_list')) > 8) or ((sum(rail.result('get_timeoff_hours_list')) + int(dag_run.conf["Timeoffhrs"])) > 8) or (dag_run.conf['Action'] == 'UPDATE')


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


def calculate_delta_hours_for_update(dag_run):
    if dag_run.conf["Action"] != "UPDATE":
        return {"status": False, "message": "Delta calculation only applies to UPDATE operations"}

    # Get existing time-off details using the unique ID (this comes from get_time_off_details_on_unique_id task)
    existing_booking_details = rail.result('get_time_off_details_on_unique_id')

    if not existing_booking_details or len(existing_booking_details) == 0:
        return {"status": False, "message": f"No existing booking found with Unique ID: {dag_run.conf['Uniqueid']}"}

    existing_booking = existing_booking_details[0]
    existing_booking_uri = existing_booking['timeoff_uri']

    existing_timeoffs = rail.result('get_time_off_details_for_user_and_date_range')

    current_booking = None
    for timeoff in existing_timeoffs:
        if timeoff.get('uri') == existing_booking_uri:
            current_booking = timeoff
            break

    if not current_booking:
        return {"status": False, "message": f"Could not retrieve current hours for booking with Unique ID: {dag_run.conf['Uniqueid']}"}

    existing_hours = float(current_booking['totalDuration']['calendarDayDuration']['hours'])
    delta_hours = float(dag_run.conf["Timeoffhrs"])
    new_hours = existing_hours + delta_hours

    # Validate new hours - negative values are allowed as deltas, but final result must be positive
    if new_hours <= 0:
        return {
            "status": False,
            "message": f"Delta calculation would result in {new_hours} hours (invalid - final hours must be positive)",
            "existing_hours": existing_hours,
            "delta_hours": delta_hours,
            "new_hours": new_hours
        }

    # HMH only passes 4 or 8 hour delta values, ensure final result is valid (4 or 8 hours only)
    if new_hours not in [4.0, 8.0]:
        return {
            "status": False,
            "message": f"Delta calculation results in {new_hours} hours, but only 4 or 8 hours are valid final values",
            "existing_hours": existing_hours,
            "delta_hours": delta_hours,
            "new_hours": new_hours
        }

    # Format delta display with proper sign
    delta_sign = "+" if delta_hours >= 0 else ""
    return {
        "status": True,
        "message": f"UPDATE will change from {existing_hours} to {new_hours} hours (delta: {delta_sign}{delta_hours})",
        "existing_hours": existing_hours,
        "delta_hours": delta_hours,
        "new_hours": new_hours,
        "existing_booking_uri": existing_booking_uri
    }

def check_for_failure():
    if len(rail.result('gather_child_error_data')) > 0:
        if any((True for response in rail.result('gather_child_error_data') if response.startswith("artifact"))):
            return True
    return False

def get_filtered_time_off_details_on_booking_id(response, dag_run):
    request_id = str(dag_run.conf['Uniqueid'])
    return list(filter(lambda x: x['unique_id'] == request_id, map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "timeoff_type": row['cells'][1]['textValue'],
        "timeoff_type_uri": row['cells'][1]['uri'],
        "unique_id": row['cells'][2]['textValue'],
        "timeoff_start_date": str(row['cells'][3]['dateValue']['year'])+'-'+str(row['cells'][3]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][3]['dateValue']['day']).zfill(2),
        "timeoff_end_date": str(row['cells'][4]['dateValue']['year'])+'-'+str(row['cells'][4]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][4]['dateValue']['day']).zfill(2)
    }, response['rows'])))
