from datetime import datetime
import uuid
import rail

null = None

def get_all_timeoff_types_payload():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:time-off-type-list-column:name",
            "urn:replicon:time-off-type-list-column:description",
            "urn:replicon:time-off-type-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": null
    }

def get_conf_payload(item):
    users_report_data = rail.load_all_records(rail.result("users_report_data_collection"))
    start_date_object = datetime.strptime(item["date"], "%Y/%m/%d")
    return {
        "employee_id": item["sfid"],
        "timeoff_type": item["timeoff_type"],
        "start_date": item["date"],
        "action": item["deletion_marker"],
        "user_uri": " ".join(list(map(lambda data: data["useruri"], filter(lambda data: data["employeeid"] == item["sfid"], users_report_data)))),
        "timeoff_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeofftypes"), "timeoff_name", item["timeoff_type"], "timeoff_uri"),
        "timeoff_status": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeofftypes"), "timeoff_name", item["timeoff_type"], "status"),
        "timeoff_name": null,
        "timeoff_hrs": {
            "hours": item["duration"].split(".")[0] if item["duration"] and "." in item["duration"]
                else (item["duration"] if item["duration"] and "." not in item["duration"] else "0"),
            "minutes": item["duration"].split(".")[-1] if item["duration"] and "." in item["duration"] else "0"
        },
        "duration": item["duration"],
        "end_date": item["date"],
        "start_date_in_format": {
            "day": start_date_object.strftime("%d"),
            "month": start_date_object.strftime("%m"),
            "year": start_date_object.strftime("%Y")
        },
        "timeoff_start_end_time": {
            "start_time_hrs": item["start_time"].split(".")[0] if item["start_time"] and "." in item["start_time"]
                else (item["start_time"] if item["start_time"] and "." not in item["start_time"] else "0"),
            "start_time_mins": item["start_time"].split(".")[-1] if item["start_time"] and "." in item["start_time"] else "0",
            "end_time_hrs": item["end_time"].split(".")[0] if item["end_time"] and "." in item["end_time"]
                else (item["end_time"] if item["end_time"] and "." not in item["end_time"] else "0"),
            "end_time_mins": item["end_time"].split(".")[-1] if item["end_time"] and "." in item["end_time"] else "0"
        },
        "type": item["type"],
        "log": rail.result("create_log")
    }

def get_timeoff_date(dag_run):
    return {
        "year": dag_run.conf["start_date_in_format"]["year"],
        "month": dag_run.conf["start_date_in_format"]["month"],
        "day": dag_run.conf["start_date_in_format"]["day"]
    }

def get_timeoff_end_details(dag_run):
    return {
        "date": get_timeoff_date(dag_run),
        "timeOffDay": {
            "hour": dag_run.conf["timeoff_start_end_time"]["end_time_hrs"],
            "minute": dag_run.conf["timeoff_start_end_time"]["end_time_mins"],
            "second": "0"
        },
        "relativeDuration": null,
        "specificDuration": null
    }

def get_create_timeoff_payload(dag_run, booking_type):
    if dag_run.conf["type"] == 'F':
        relative_duration = "urn:replicon:time-off-relative-duration:full-day"
        get_start_timeoff_day = null
        timeoff_end = null
        start_specific_duration = null

    elif dag_run.conf["type"] == 'P':
        relative_duration = "urn:replicon:time-off-relative-duration:half-day"
        get_start_timeoff_day = null
        timeoff_end = null
        start_specific_duration = null

    elif dag_run.conf["type"] == 'N':
        relative_duration = null
        if dag_run.conf["timeoff_start_end_time"]["start_time_hrs"] and dag_run.conf["timeoff_start_end_time"]["start_time_hrs"] != "0":
            get_start_timeoff_day = {
                "hour": dag_run.conf["timeoff_start_end_time"]["start_time_hrs"],
                "minute": dag_run.conf["timeoff_start_end_time"]["start_time_mins"],
                "second": "0"
            }
            timeoff_end = get_timeoff_end_details(dag_run)
            start_specific_duration = {
                "hours": dag_run.conf["timeoff_hrs"]["hours"],
                "minutes": dag_run.conf["timeoff_hrs"]["minutes"],
                "seconds": "0",
                "milliseconds": "0",
                "microseconds": "0"
            }
        else:
            get_start_timeoff_day = null
            timeoff_end = null
            start_specific_duration = {
                "hours": str(int(dag_run.conf["timeoff_hrs"]["hours"])),
                "minutes": str(int(dag_run.conf["timeoff_hrs"]["minutes"])),
                "seconds": "0",
                "milliseconds": "0",
                "microseconds": "0"
            }

    return {
        "timeOff": {
            "target": {
                "uri": rail.result(f"createdraft_timeoffbooking_for_user_type_{ booking_type }")
            },
            "owner": {
                "uri": dag_run.conf["user_uri"],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf["timeoff_uri"],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_timeoff_date(dag_run),
                    "timeOfDay": get_start_timeoff_day,
                    "relativeDuration": relative_duration,
                    "specificDuration": start_specific_duration
                },
                "timeOffEnd": timeoff_end
            },
            "userExplicitEntries": [],
            "comments": "Added by Replicon Integration",
            "customFieldValues": []
        }
}

def get_approve_timeoff_booking_payload(booking_type):
    return {
        "timeOffUri": rail.result(f"publish_timeoff_draft_for_user_type_{ booking_type }")["uri"],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Replicon Integration"
    }

def get_time_off_details_for_user_and_date_range_payload(dag_run):
    return {
        "userUri": dag_run.conf["user_uri"],
        "dateRange": {
          "startDate": {
            "year": dag_run.conf["start_date_in_format"]["year"],
            "month": dag_run.conf["start_date_in_format"]["month"],
            "day": dag_run.conf["start_date_in_format"]["day"]
          },
          "endDate":  {
            "year": dag_run.conf["start_date_in_format"]["year"],
            "month": dag_run.conf["start_date_in_format"]["month"],
            "day": dag_run.conf["start_date_in_format"]["day"]
           },
          "relativeDateRangeUri": null,
          "relativeDateRangeAsOfDate": null
        }
    }

def delete_timeoff_payload_1(dag_run):
    return {
        "timeOffUri": ''.join(list(map(lambda data: data["uri"],
            filter(lambda data: data["timeOffType"]["name"] == dag_run.conf["timeoff_type"] and
                data["totalDuration"]["decimalWorkdays"] == int(float(dag_run.conf["duration"])) if dag_run.conf["type"]=="F"
                    else data["totalDuration"]["decimalWorkdays"] == float(dag_run.conf["duration"]),
                        rail.result("get_time_off_details_for_user_and_date_range_2")))))
    }

def delete_timeoff_payload_2(dag_run):
    return {
        "timeOffUri": ''.join(list(map(lambda data: data["uri"],
            filter(lambda data: data["timeOffType"]["name"] == dag_run.conf["timeoff_type"] and
                data["totalDuration"]["calendarDayDuration"]["hours"] == int(dag_run.conf["timeoff_hrs"]["hours"]) and
                    data["totalDuration"]["calendarDayDuration"]["minutes"] == int(dag_run.conf["timeoff_hrs"]["minutes"]),
                        rail.result("get_time_off_details_for_user_and_date_range_2")))))
    }
