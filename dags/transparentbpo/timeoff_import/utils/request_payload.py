import rail
from uuid import uuid4
from transparentbpo.timeoff_import import config
from transparentbpo.timeoff_import.utils import custom_methods

null = None


def process_each_timeoff_from_bamboohr(item, enabled_timeoffs_in_replicon):
    """
    Add enabled timeoff types to each BambooHR timeoff record.
    Filters to only include allowed timeoff types from config.

    Args:
        item: BambooHR timeoff record
        enabled_timeoffs_in_replicon: List of enabled timeoff types from Replicon

    Returns:
        Modified item with filtered enabled timeoff types
    """
    filtered_timeoffs = [
        timeoff.get('displayText') for timeoff in enabled_timeoffs_in_replicon
        if timeoff.get('displayText') in config.allowed_timeoff_types
    ]
    item["required_timeoff_types_in_replicon"] = filtered_timeoffs
    item["timeoff_id"] = item.get("id", "")
    item["bamboohr_id"] = item.get("employeeId", "")
    return item


def process_each_timeoff_from_bamboohr_with_raw(item, enabled_timeoffs_in_replicon):
    """
    Process flattened timeoff record from QueryCollectionOperator and rebuild
    the original BambooHR structure for child DAG processing.

    Args:
        item: Flattened timeoff record from QueryCollectionOperator
        enabled_timeoffs_in_replicon: List of enabled timeoff types from Replicon

    Returns:
        Rebuilt item with original BambooHR structure and filtered enabled timeoff types
    """
    filtered_timeoffs = [
        timeoff.get('displayText') for timeoff in enabled_timeoffs_in_replicon
        if timeoff.get('displayText') in config.allowed_timeoff_types
    ]

    # Rebuild the original BambooHR structure from flattened data
    rebuilt_item = {
        'timeoff_id': item.get('id', ''),
        'bamboohr_id': item.get('employeeid', ''),
        'name': item.get('name', ''),
        'start': item.get('start', ''),
        'end': item.get('end', ''),
        'created': item.get('created', ''),
        'status': {
            'status': item.get('status', ''),
            'lastChanged': item.get('lastchanged', '')
        },
        'type': {
            'id': item.get('typeid', ''),
            'name': item.get('typename', '')
        },
        'amount': {
            'unit': item.get('amountunit', ''),
            'amount': item.get('amount', '')
        },
        'notes': {
            'manager': item.get('notes', '')
        },
        'required_timeoff_types_in_replicon': filtered_timeoffs
    }

    return rebuilt_item


def process_each_timeoff_record(item, input_data):
    """
    Build configuration for processing each timeoff record.
    Extracts user URI and timeoff URI from BulkGetUsers3 response.

    Args:
        item: Individual timeoff booking with date and hours
        input_data: Parent timeoff data from BambooHR

    Returns:
        Dictionary with all required fields for timeoff processing
    """
    user_data_response = rail.result('get_user_data')
    employee_details = rail.result('get_employee_details')

    return {
        'timeoff_id': input_data['timeoff_id'],
        'bamboohr_id': input_data['bamboohr_id'],
        'employee_id': employee_details['employeeNumber'],
        'status': input_data['status']["status"],
        'last_changed': input_data['status']['lastChanged'],
        'name': input_data['name'],
        'start': input_data['start'],
        'end': input_data['end'],
        'created': input_data['created'],
        'typeid': input_data['type']['id'],
        'type_name': input_data['type']['name'],
        'amount_unit': input_data['amount']['unit'],
        'amount': input_data['amount']['amount'],
        'booking_date': item['date'],
        'booking_hour': item['hours'],
        'notes': input_data['notes'].get('manager', ''),
        'user_uri': custom_methods.extract_user_uri(user_data_response),
        'timeoff_uri': custom_methods.get_timeoff_uri_from_user_data(user_data_response, input_data['type']['name']),
        'scheduled_hrs': custom_methods.manipulate_schedule_hrs(item['date'], rail.result('get_user_scheduled_hours_in_date_range')['schedulelist']),
        'log': rail.result('create_log')
    }


# ============================================================================
# process_timeoff_booking_child.py payloads
# ============================================================================

def get_user_scheduled_hours_in_date_range_payload(dag_run, date_format):
    """
    Build payload for GetScheduledHoursInDateRange API call.

    Args:
        dag_run: Airflow DagRun object
        date_format: Date format string for parsing

    Returns:
        Dictionary payload for the API call
    """
    return {
        "userUri": custom_methods.extract_user_uri(rail.result('get_user_data')),
        "dateRange": {
            "startDate": rail.parse_date(dag_run.conf["start"], date_format),
            "endDate": rail.parse_date(dag_run.conf["end"], date_format),
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


# ============================================================================
# process_each_entry_child.py payloads
# ============================================================================

def get_timesheet_for_date2_payload(dag_run):
    """
    Build payload for GetTimesheetForDate2 API call.

    Args:
        dag_run: Airflow DagRun object

    Returns:
        Dictionary payload for the API call
    """
    return {
        "userUri": dag_run.conf['user_uri'],
        "date": rail.parse_date(dag_run.conf['booking_date'], "%Y-%m-%d"),
        "timesheetGetOptionUri": null
    }


def get_timesheet_details_payload(timesheet_result_task_id):
    """
    Build payload for GetTimesheetDetails API call.

    Args:
        timesheet_result_task_id: Task ID to get timesheet result from

    Returns:
        Dictionary payload for the API call
    """
    return {
        "timesheetUri": custom_methods.get_timesheet_uri(rail.result(timesheet_result_task_id))
    }


def reopen_timesheet_payload(timesheet_result_task_id):
    """
    Build payload for Reopen timesheet API call.

    Args:
        timesheet_result_task_id: Task ID to get timesheet result from

    Returns:
        Dictionary payload for the API call
    """
    return {
        "timesheetUri": custom_methods.get_timesheet_uri(rail.result(timesheet_result_task_id)),
        "unitOfWorkId": str(uuid4()),
        "comments": "Reopened by Replicon Integration"
    }


def get_timeoff_details_for_user_and_date_range_payload(dag_run):
    """
    Build payload for GetTimeOffDetailsForUserAndDateRange2 API call.

    Args:
        dag_run: Airflow DagRun object

    Returns:
        Dictionary payload for the API call
    """
    return {
        "userUri": dag_run.conf['user_uri'],
        "dateRange": {
            "startDate": rail.parse_date(dag_run.conf['booking_date'], "%Y-%m-%d"),
            "endDate": rail.parse_date(dag_run.conf['booking_date'], "%Y-%m-%d"),
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def put_time_off2_payload(dag_run, draft_task_id, include_specific_duration=False):
    """
    Build payload for PutTimeOff2 API call.

    Args:
        dag_run: Airflow DagRun object
        draft_task_id: Task ID to get the draft URI from
        include_specific_duration: Whether to include specific duration for vacation type

    Returns:
        Dictionary payload for the API call
    """
    specific_duration = null
    relative_duration = "urn:replicon:time-off-relative-duration:full-day"
    if include_specific_duration:
        specific_duration = {
            "hours": 0,
            "minutes": 0,
            "seconds": int(float(dag_run.conf['booking_hour']) * 3600),
            "milliseconds": "0",
            "microseconds": "0"
        }
        relative_duration = null

    return {
        "timeOff": {
            "target": {
                "uri": rail.result(draft_task_id)
            },
            "owner": {
                "uri": dag_run.conf['user_uri'],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoff_uri'],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": rail.parse_date(dag_run.conf['booking_date'], '%Y-%m-%d'),
                    "timeOfDay": null,
                    "relativeDuration": relative_duration,
                    "specificDuration": specific_duration
                },
                "timeOffEnd": null
            },
            "userExplicitEntries": [],
            "comments": "Added by Replicon Integration",
            "customFieldValues": []
        }
    }


def force_approve_timeoff_payload(publish_task_id):
    """
    Build payload for Force Approve timeoff API call.

    Args:
        publish_task_id: Task ID to get published timeoff URI from

    Returns:
        Dictionary payload for the API call
    """
    return {
        "timeOffUri": rail.result(publish_task_id)['uri'],
        "unitOfWorkId": str(uuid4()),
        "comments": "Approved by Replicon Integration"
    }


def submit_timesheet_payload(timesheet_details_task_id):
    """
    Build payload for Submit2 timesheet API call.

    Args:
        timesheet_details_task_id: Task ID to get timesheet details from

    Returns:
        Dictionary payload for the API call
    """
    return {
        "timesheetUri": rail.result(timesheet_details_task_id)['uri'],
        "unitOfWorkId": str(uuid4()),
        "comments": "Submitted by Replicon Integration"
    }


def force_approve_timesheet_payload(timesheet_details_task_id):
    """
    Build payload for Force Approve timesheet API call.

    Args:
        timesheet_details_task_id: Task ID to get timesheet details from

    Returns:
        Dictionary payload for the API call
    """
    return {
        "timesheetUri": rail.result(timesheet_details_task_id)['uri'],
        "unitOfWorkId": str(uuid4()),
        "comments": "Force Approved by Replicon Integration"
    }
