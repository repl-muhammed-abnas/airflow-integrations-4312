from datetime import datetime
import hashlib
import uuid
import rail
from sqlalchemy import null
from rail.lib.ecid import get_dagrun_ecid
null = None

date_format = "%d/%m/%Y"


def get_shift_summary_payload(dag_run):
    week_start_date = datetime.strptime(
        dag_run.conf['week_start_date'], '%Y%m%d')
    week_end_date = datetime.strptime(
        dag_run.conf['week_end_date'], '%Y%m%d')
    return {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [dag_run.conf['user_uri']]
        },
        "shiftSearch": None,
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": {
                "year": week_start_date.year,
                "month": week_start_date.month,
                "day": week_start_date.day
            },
            "endDate": {
                "year": week_end_date.year,
                "month": week_end_date.month,
                "day": week_end_date.day
            },
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }


def get_timesheet_uri(dag_run):
    timesheet_uri = dag_run.conf.get('time_sheet_uri')
    if not timesheet_uri:
        timesheet_uri = rail.result('get_timesheet_for_date')[
            'timesheet']['uri']
    return {
        "timesheetUri": timesheet_uri
    }


def get_re_open_request(dag_run):
    timesheet_uri = dag_run.conf.get('time_sheet_uri')
    if not timesheet_uri:
        timesheet_uri = rail.result('get_timesheet_for_date')[
            'timesheet']['uri']
    return {
        "timesheetUri": timesheet_uri,
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "reopend by integration since the time off was modified or booked"
    }


def get_booking_info(dag_run, item):
    return {
        **item,
        "create_file_processing_log": dag_run.conf['create_file_processing_log'],
        "master_ecid": dag_run.conf['master_ecid']
    }


def get_assignment_effective_info(dag_run, item):
    return {
        "effective_date": item['effective_date'],
        "pattern": item['pattern'],
        "shift_name": item['shift_name'],
        "start_date": item['start_date'],
        "end_date": item['end_date'],
        "user_uri": item['user_uri'],
        "timeoff_type_name": item['timeoff_type_name'],
        "user_name": item['user_name'],
        "user_assignment_history": dag_run.conf['user_assignment_history'],
        "shift_referance": hashlib.md5((item['user_uri']+","
                                        + item['pattern']+","
                                        + item['effective_date']).encode()).hexdigest()
    }


def get_timeoff_shift_summary_payload():
    effective_range = rail.result('get_timeoff_date_range')
    start_date = datetime.strptime(effective_range['start_date'], '%Y%m%d')
    end_date = datetime.strptime(effective_range['end_date'], '%Y%m%d')
    user_data = rail.load_all_records(rail.result('query_userdata'))
    user_uris = [user["user_uri"] for user in user_data]
    return {
        "userUris": user_uris,
        "dateRange": {
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_holiday_series_payload():
    effective_range = rail.result('get_timeoff_date_range')
    start_date = datetime.strptime(effective_range['start_date'], '%Y%m%d')
    end_date = datetime.strptime(effective_range['end_date'], '%Y%m%d')
    user_data = rail.load_all_records(rail.result('query_userdata'))
    user_uris = [user["user_uri"] for user in user_data]
    return {
        "userUris": user_uris,
        "dateRange": {
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_timeoff_holiday_info(dag_run, item):
    return {
        "effective_dates": item["effective_dates"],
        "week_start_date": item["week_start_date"],
        "week_end_date": item["week_end_date"],
        "user_name": item["user_name"],
        "user_uri": item["user_uri"],
        "timeoff_type_name": item["timeoff_type_name"],
        "booking_start_date": item['booking_start_date'],
        "booking_end_date": item['booking_end_date'],
        "master_ecid": get_dagrun_ecid(dag_run)
    }


def get_bulk_assignment_request():
    shift_to_assign_payloads = []
    shifts_to_assign = rail.result('get_shift_actions')['shifts_to_assign']
    for shift_to_assign in shifts_to_assign:
        effective_date = datetime.strptime(
            shift_to_assign['effective_date'], '%Y%m%d')
        shift_to_assign_info = {
            "date": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "target": {
                "uri": None
            },
            "shift": {
                "name": shift_to_assign['shift_name']
            },
            "user": {
                "uri": shift_to_assign['user_uri']
            },
            "note": "Assigned By Integration",
            "publishState": "urn:replicon:shift-assignment-publish-state:published"
        }

        shift_to_assign_payloads.append(shift_to_assign_info)
    return {
        "assignments": shift_to_assign_payloads,
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_assignment_uris_to_bulk_delete():
    shifts_to_delete = rail.result('get_shift_actions')['shifts_to_delete']
    return {
        "shiftAssignmentUris": list(shifts_to_delete)
    }
