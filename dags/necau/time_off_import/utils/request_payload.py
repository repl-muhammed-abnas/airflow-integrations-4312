from datetime import datetime, timedelta
import math
import hashlib
import uuid
import rail
from sqlalchemy import null
null = None

date_format = "%d/%m/%Y"


def get_shift_summary_payload(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['effective_date'], '%Y%m%d')

    return {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [dag_run.conf['user_uri']]
        },
        "shiftSearch": None,
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "endDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }


def get_timeoff_approve_request(dag_run):
    return {
        "timeOffUri": dag_run.conf['booking_uri'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Integration"
    }


def get_timeoff_type_payload(dag_run):
    timeoff_request = {
        "timeOffType": {
            "target": {
                "uri": rail.result('create_new_timeoff_type_draft'),
                "name": null
            },
            "name": dag_run.conf['leave_description'],
            "description": null,
            "enabled": "true",
            "timeOffBalanceTrackingOptionUri": "urn:replicon:time-off-balance-tracking-option:do-not-track-balance",
            "minimumTimeOffIncrementPolicyUri": "urn:replicon:policy:time-off:minimum-increment:no-minimum",
            "startEndTimeSpecificationRequirementUri": null,
            "measurementUnitUri": "urn:replicon:time-off-measurement-unit:hours",
            "timeOffDisplayFormatUri": null,
            "payCodeUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":pay-code:3"
        }
    }

    return timeoff_request


def get_publish_timeoff_request():
    return {
        "timeOffType": rail.result('create_new_timeoff_type_draft')
    }


def get_assing_validation_payload():
    validation_script_info = rail.result('get_all_time_off_validation_scripts')
    validation_script_uri = rail.find_first_by_attr_and_get_attr(
        validation_script_info, 'displayText', 'Time off booking not allowed', "uri")

    timeoff_validation_request = {
        "timeOffTypeBookingPolicy": {
            "target": {
                "uri": rail.result('publish_timeoff')['uri'],
                "name": null
            },
            "policyValues": [
                {
                    "policyKeyUri": "urn:replicon:time-off-booking-policy-keys:validation-rule",
                    "policyValue": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": [
                            {
                                "uri": validation_script_uri,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        ]
                    }
                },
                {
                    "policyKeyUri": "urn:replicon:time-off-booking-policy-keys:object-extension-definition",
                    "policyValue": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ]
        }
    }

    return timeoff_validation_request


def get_request_timesheetdate(dag_run):
    start_date = datetime.strptime(dag_run.conf['start_date'], date_format)
    return {
        "userUri": dag_run.conf['user_uri'],
        "date": {
            "year": start_date.year,
            "month": start_date.month,
            "day": start_date.day
        },
        "timesheetGetOptionUri": null
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


def get_timeoff_details_request(dag_run):
    start_date = datetime.strptime(dag_run.conf['start_date'], date_format)
    end_date = datetime.strptime(dag_run.conf['end_date'], date_format)
    return {
        "userUri": dag_run.conf['user_uri'],
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


def get_all_custom_field_request():
    return {
        "objectUri": rail.result('get_custom_field_groups')['uri']
    }


def get_timeoff_delete_request(dag_run):
    return {
        "timeOffUri": dag_run.conf['booking_uri']
    }


def get_assignment_request(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['effective_date'], '%Y%m%d')
    return {
        "assignment": {
            "date": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "target": {
                "uri": None
            },
            "shift": {
                "uri": None,
                "name": dag_run.conf['shift_name']
            },
            "user": {
                "uri": dag_run.conf['user_uri'],
                "loginName": None,
                "parameterCorrelationId": None
            },
            "startTime": None,
            "endTime": None,
            "note": "Assigned By Integration",
            "publishState": "urn:replicon:shift-assignment-publish-state:published",
            "extensionFieldValues": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_shift_draft_uri():
    return {
        "shiftAssignmentDraftUri": rail.result('create_new_shift_draft')
    }


def get_timeoff_single_configure_request(caller, start_date, hours_taken_min, hours_taken_sec):
    return {
        "timeOffUri": rail.result(f'create_new_time_off_draft_{caller}'),
        "date": {
            "date": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day
            },
            "timeOfDay": None,
            "relativeDuration": None,
            "specificDuration": {
                "hours": "0",
                "minutes": hours_taken_min,
                "seconds": hours_taken_sec,
                "milliseconds": "0",
                "microseconds": "0"
            }
        }
    }


def get_timeoff_multi_configure_request(caller, start_date, end_date):
    return {
        "timeOffUri": rail.result(f'create_new_time_off_draft_{caller}'),
        "startDate": {
            "date": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day
            },
            "timeOfDay": None,
            "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
            "specificDuration": None
        },
        "endDate": {
            "date": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            },
            "timeOfDay": None,
            "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
            "specificDuration": None
        }
    }


def get_configure_timeoff_end_point(caller):
    if caller in ["single_day", "multi_day_with_partial1"]:
        return "/services/TimeOffService1.svc/ConfigureSingleDayTimeOff"
    return "/services/TimeOffService1.svc/ConfigureMultiDayTimeOff"


def round_hours(hours):
    last_digit = hours[-1]
    if last_digit == '5':
        return round(math.ceil(float(hours) * 1000.0) / 1000.0, 3)
    return round(float(hours), 3)


def get_configure_request_based_timeoff_days(dag_run, caller):
    start_date = datetime.strptime(
        dag_run.conf['start_date'], date_format)
    hours_taken = float(dag_run.conf['hours_taken']) * 60
    hours_taken_min = float((str(hours_taken)).split('.', maxsplit=1)[0])
    hours_taken_sec_str = (str(hours_taken)).split('.', maxsplit=1)[-1]
    hours_taken_sec = round_hours(str(0)+"." + str(hours_taken_sec_str)) * 60

    if caller == "single_day":
        return get_timeoff_single_configure_request(caller, start_date, int(hours_taken_min), int(hours_taken_sec))
    if caller == 'multi_day':
        end_date = datetime.strptime(
            dag_run.conf['end_date'], date_format)
        return get_timeoff_multi_configure_request(caller, start_date, end_date)
    if caller == 'multi_day_with_partial1':
        hours_taken_days = (float(dag_run.conf['hours_taken']) - (
            (float(dag_run.conf['hours_taken']) / float(dag_run.conf['days_taken'])) * int(float(dag_run.conf['days_taken'])))) * 60
        split_hours_taken_days = str(hours_taken_days).split('.', maxsplit=1)
        partial_booking_minutes = float(split_hours_taken_days[0])
        partial_booking_seconds_str = str(hours_taken_days).split(
            '.')[1] if len(split_hours_taken_days) == 2 else 0
        partial_booking_seconds = float(
            str(0) + "." + partial_booking_seconds_str)
        partial_booking_seconds = round_hours(
            str(0) + "." + partial_booking_seconds_str) * 60
        return get_timeoff_single_configure_request(caller, start_date, int(partial_booking_minutes), int(partial_booking_seconds))
    end_date = datetime.strptime(dag_run.conf['end_date'], date_format)
    next_day = start_date + timedelta(days=1)
    return get_timeoff_multi_configure_request(caller, next_day, end_date)


def get_timeoff_draft_uri(caller):
    return {
        "timeOff": rail.result(f'create_new_time_off_draft_{caller}')
    }


def get_update_custom_key_request(custom_key_value, caller, custom_key_name):
    all_custom_info = rail.result('get_all_custom_fields')
    custom_field_uri = rail.find_first_by_attr_and_get_attr(
        all_custom_info, 'displayText', custom_key_name, "uri")
    return {
        "objectUri": rail.result(f'publish_timeoff_draft_{caller}')['uri'],
        "customFieldUri": custom_field_uri,
        "value": custom_key_value
    }


def get_timeoff_action_request(caller, action='approve'):
    return {
        "timeOffUri": rail.result(f'publish_timeoff_draft_{caller}')['uri'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Integration" if action == 'approve' else ""
    }


def get_assignment_uris_to_delete():
    shifts_to_delete = rail.result('shift_assignment_category')[0]
    return {
        "shiftAssignmentUris": list(shifts_to_delete)
    }


def get_update_timeoff_request(dag_run, caller):
    existingtimeoff = rail.result('get_all_time_off_types')
    newly_created_timeoff_info = rail.result('publish_timeoff')
    timeoff_type_uri = newly_created_timeoff_info['uri'] if newly_created_timeoff_info else None
    if not timeoff_type_uri:
        # timeoff_type_uri = rail.find_first_by_attr_and_get_attr(
        #     existingtimeoff, 'displayText', dag_run.conf['leave_description'], "uri")

        timeoff_type_info = list(filter(
            lambda data: dag_run.conf['leave_description'] and
            data['displayText'].lower() == dag_run.conf['leave_description'].lower(), existingtimeoff))
        timeoff_type_uri = timeoff_type_info[0]['uri']
    return {
        "timeOffUri": rail.result(f'create_new_time_off_draft_{caller}'),
        "timeOffTypeUri": timeoff_type_uri
    }


def get_timeoff_comments(caller):
    return {
        "timeOffUri": rail.result(f'create_new_time_off_draft_{caller}'),
        "comments": "Created by Integration"
    }


def get_user_and_time_off_info(dag_run, item):
    user_data = rail.load_all_records(dag_run.conf['query_userdata'])
    users_by_staff_member = list(filter(
        lambda data: data['prev_employee_number'] == item['staff_member'], user_data))
    user_uri, user_status, is_shift_user, schedule_name, shift_user_udf_name, user_email, supervisor_email = None, None, None, None, None, None, None
    if users_by_staff_member and len(users_by_staff_member) > 0:
        user_by_staff_member = users_by_staff_member[0]
        user_uri = user_by_staff_member['user_uri']
        user_status = user_by_staff_member['user_status']
        is_shift_user = user_by_staff_member['auto_schedule_option']
        schedule_name = user_by_staff_member['auto_schedule_shift_name']
        shift_user_udf_name = user_by_staff_member['auto_schedule_shift_name']
        user_email = user_by_staff_member['user_email']
        supervisor_email = user_by_staff_member['supervisor_email']

    file_name = dag_run.conf['file_name']
    preferred_name = item["preferred_name"] if 'Requests' in file_name else item["preferred_name_1"]
    form_code = item["form_code_1"] if 'Cancellations' in file_name else item["form_code"]
    request_key = item["request_key"] if 'Requests' in file_name else item[
        "request_key_1"] if "Approved" in file_name else item["request_key_2"]
    seq_no = item["seq_no"] if 'Requests' in file_name else item["seq_no_1"] if "Approved" in file_name else item["seq_no_2"]
    leave_type = item["leave_type"] if 'Requests' in file_name else item[
        "leave_type_1"] if "Approved" in file_name else item["leave_type_2"]
    leave_description = item["leave_description"] if 'Requests' in file_name else item[
        "leave_description_1"] if "Approved" in file_name else item["leave_description_2"]
    start_date = item["start_date"] if 'Requests' in file_name else item[
        "start_date_1"] if "Approved" in file_name else item["start_date_2"]
    end_date = item["end_date"] if 'Requests' in file_name else item["end_date_1"] if "Approved" in file_name else item["end_date_2"]
    action_status = item["action_status"] if 'Requests' in file_name else item[
        "action_status_1"] if "Approved" in file_name else item["action_status_2"]
    days_taken = item["days_taken"] if 'Requests' in file_name else item["days_taken_1"] if "Approved" in file_name else ""
    hours_taken = item["hours_taken"] if 'Requests' in file_name else item["hours_taken_1"] if "Approved" in file_name else ""
    return {
        "staff_member": item["staff_member"] if item["staff_member"] else "",
        "surname": item["surname"] if item["surname"] else "",
        "preferred_name": preferred_name if preferred_name else "",
        "form_code": form_code if form_code else "",
        "form_description": item.get("form_description") if item.get("form_description") else "",
        "request_key": request_key if request_key else "",
        "creation_date": item.get("creation_date") if item.get("creation_date") else "",
        "creation_time": item.get("creation_time") if item.get("creation_time") else "",
        "seq_no": seq_no if seq_no else "",
        "leave_type": leave_type if leave_type else "",
        "leave_description": leave_description if leave_description else "",
        "start_date": start_date if start_date else "",
        "end_date": end_date if end_date else "",
        "action_status": action_status if action_status else "",
        "days_taken": days_taken if days_taken else 0.0,
        "hours_taken": hours_taken if hours_taken else 0.0,
        "user_uri": user_uri if user_uri else "",
        "user_status": user_status if user_status else "",
        "schedule_name": schedule_name if schedule_name else "",
        "is_shift_user": is_shift_user if is_shift_user else "",
        "shift_user_udf_name": shift_user_udf_name if shift_user_udf_name else "",
        "user_email": user_email if user_email else "",
        "supervisor_email": supervisor_email if supervisor_email else "",
        "master_ecid": dag_run.conf['master_ecid'],
        "create_file_processing_log": rail.result('create_file_processing_log'),
        "user_assignment_history": rail.result('create_shift_assignment_log')
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


def get_errored_timeoff_delete_request():
    timeoff_draft_multi_day_with_partial1 = rail.result(
        'publish_timeoff_draft_multi_day_with_partial1')
    timeoff_draft_multi_day_with_partial2 = rail.result(
        'publish_timeoff_draft_multi_day_with_partial2')
    timeoff_draft_multi_day = rail.result('publish_timeoff_draft_multi_day')
    timeoff_draft_single_day = rail.result('publish_timeoff_draft_single_day')

    timeoff_draft_info = timeoff_draft_multi_day or timeoff_draft_single_day or timeoff_draft_multi_day_with_partial1 or timeoff_draft_multi_day_with_partial2
    return {
        "timeOffUri": timeoff_draft_info['uri']
    }
