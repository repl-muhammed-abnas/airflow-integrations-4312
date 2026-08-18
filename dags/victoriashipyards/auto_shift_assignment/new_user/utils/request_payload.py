from datetime import datetime
import uuid
from calendar import monthrange
from dateutil.relativedelta import relativedelta

import rail
import pendulum

def get_week_start_end_date():
    start_date = pendulum.now('PST8PDT')
    enddate_from_today = start_date + relativedelta(months=6)
    last_day_of_month = monthrange(enddate_from_today.year, enddate_from_today.month)
    end_date = datetime(enddate_from_today.year, enddate_from_today.month, last_day_of_month[1])
    return  start_date, end_date

def get_put_shift_payload():
    assignments = rail.load_all_records(rail.result(
    "get_final_shift_assignment_list"))
    data= {
        "assignments": assignments,
        "unitOfWorkId": str(uuid.uuid4())
    }
    return data

def get_shift_assignment_data_for_daily_update(item):
    startdate, end_date = get_week_start_end_date()
    shift_name = item["defaultshift"] if item["defaultshift"] else "VSL System Shift"
    data = {
        "Useruri": item["useruri"],
        "Startdate": startdate.strftime("%Y-%m-%d"),
        "Enddate": end_date.strftime("%Y-%m-%d"),
        "Shiftname": shift_name,
        "Startdateday": startdate.strftime("%d"),
        "Startdatemonth": startdate.strftime("%m"),
        "Startdateyear": startdate.strftime("%Y"),
        "Enddateday" : end_date.strftime("%d"),
        "Enddatemonth": end_date.strftime("%m"),
        "Enddateyear": end_date.strftime("%Y"),
        "Username": item["username"],
        "Loginname": item["loginname"],
        "Type":  "VSL"
    }
    return data

def get_assignment_uris():
    shift_details = rail.load_all_records(rail.result(
    "get_assigned_shift_dates"))
    shift_uris_to_delete = [sub["assignmenturi"] for sub in shift_details]
    return {
        "shiftAssignmentUris": shift_uris_to_delete
    }

def get_shift_schedule_summary_data(dag_run):
    data = {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [
                dag_run.conf['Useruri']
            ]
        },
        "shiftSearch": None,
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": {
                "year": int(dag_run.conf['Startdateyear']),
                "month": int(dag_run.conf['Startdatemonth']),
                "day": int(dag_run.conf['Startdateday'])
            },
            "endDate": {
                "year": int(dag_run.conf['Enddateyear']),
                "month":  int(dag_run.conf['Enddatemonth']),
                "day": int(dag_run.conf['Enddateday'])
            },
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }
    return data
