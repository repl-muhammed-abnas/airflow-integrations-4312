from datetime import timedelta, datetime
import calendar
import uuid
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None


def get_request_conf(item):
    def getstartdate():
        return datetime.today()

    def getenddate():
        startdate = getstartdate()
        delta = 397 if startdate.day <= 15 else 393
        lastmonthyear = startdate + timedelta(days=delta)
        lastdayofmonth = calendar.monthrange(
            lastmonthyear.year, lastmonthyear.month)
        enddate = datetime(lastmonthyear.year,
                           lastmonthyear.month, lastdayofmonth[1])

        return {
            'enddate': enddate.strftime('%Y-%m-%d'),
            'year': enddate.year,
            'month': enddate.month,
            'day': enddate.day
        }

    return {
        "useruri": item['useruri'],
        "loginname": item['loginname'],
        "shiftname": item['regularshiftuserudf'],
        "username": item['username'],
        "startdate": getstartdate().strftime('%Y-%m-%d'),
        "enddate": getenddate()['enddate'],
        "startdateday": (getstartdate() + timedelta(days=1)).day,
        "startdatemonth": (getstartdate() + timedelta(days=1)).month,
        "startdateyear": (getstartdate() + timedelta(days=1)).year,
        "enddateday": getenddate()['day'],
        "enddatemonth": getenddate()['month'],
        "enddateyear": getenddate()['year'],
        "type": 'skip',
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
    }


def get_shift_details_payload():
    return {
        "page": 1,
        "pagesize": 1000,
        "columnUris": [
            "urn:replicon:shift-list-column:name",
            "urn:replicon:shift-list-column:is-enabled"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_shift_schedule_summary_for_user_payload(dag_run):
    return {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [
                dag_run.conf['useruri']
            ]
        },
        "shiftSearch": null,
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": {
                "year": dag_run.conf['startdateyear'],
                "month": dag_run.conf['startdatemonth'],
                "day": dag_run.conf['startdateday']
            },
            "endDate": {
                "year": dag_run.conf['enddateyear'],
                "month": dag_run.conf['enddatemonth'],
                "day": dag_run.conf['enddateday']
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_bulk_delete_for_user_payload():
    shift_details = rail.load_all_records(rail.result(
        "get_shift_schedule_summary_for_user"))
    shift_uris_to_delete = [sub['assignmenturi']
                            for sub in shift_details if sub['week_day'] != 6 and sub['week_day'] != 5]
    return {
        "shiftAssignmentUris": shift_uris_to_delete
    }


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
                    "uri": null
                },
                "shift": {
                    "uri": null,
                    "name": shift_name
                },
                "user": {
                    "uri": useruri,
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "note": "Published by shift automation",
                "publishState": "urn:replicon:shift-assignment-publish-state:published"
            }
        ]
        shift_assignments.append(each_shift)
    shift_assignments_list = [
        data for shifts in shift_assignments for data in shifts]
    return shift_assignments_list


def get_put_shift_payload():
    assignments = rail.load_all_records(rail.result(
        "add_shift_assignment_to_list"))
    data = {
        "assignments": assignments,
        "unitOfWorkId": str(uuid.uuid4())
    }
    return data
