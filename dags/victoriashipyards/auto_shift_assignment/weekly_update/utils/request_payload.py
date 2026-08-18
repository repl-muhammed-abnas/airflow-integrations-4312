from datetime import timedelta
import uuid
import rail
import pendulum

def get_week_start_end_date():
    date_mapper = {
        0 : -1,
        1 : 0,
        2 : 1,
        3 : 2,
        4 : 3,
        5 : 4,
        6 : 5}
    current_date = pendulum.now('PST8PDT')
    startdate = current_date.subtract(days=date_mapper[current_date.day_of_week])
    end_date = startdate + timedelta(days=4)
    return  startdate, end_date

def get_put_shift_payload():
    assignments = rail.load_all_records(rail.result(
    "add_shift_assignment_to_list"))
    data= {
        "assignments": assignments,
        "unitOfWorkId": str(uuid.uuid4())
    }
    return data

def get_shift_assignment_data(item):
    startdate, end_date = get_week_start_end_date()
    data = {
        'Useruri': item['useruri'],
        'Startdate': startdate.strftime("%Y-%m-%d"),
        'Enddate':end_date.strftime("%Y-%m-%d"),
        'Shiftname': item['defaultshift'],
        'Startdateday': startdate.strftime("%d"),
        'Startdatemonth': startdate.strftime("%m"),
        'Startdateyear': startdate.strftime("%Y"),
        'Enddateday' : end_date.strftime("%d"),
        'Enddatemonth': end_date.strftime("%m"),
        'Enddateyear': end_date.strftime("%Y"),
        'Username': item['username'],
        'Loginname': item['loginname'],
        'Type':  "VSL"
    }
    return data

def get_assignment_uris():
    shift_details = rail.load_all_records(rail.result(
    "get_assigned_shift_dates"))
    shift_uris_to_delete = [sub['assignmenturi'] for sub in shift_details]
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
