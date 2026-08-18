from datetime import datetime as dt, timedelta
from pendulum import now
import pytz
import rail
import uuid
from repliconinc.timeoff_sync_to_polaris import config
from repliconinc.timeoff_sync_to_polaris import instances

null = None

def _get_start_date_in_format_from_conf(dag_run):
    """
    Return a dict with keys 'year', 'month', 'day'.
    Prefer `dag_run.conf['start_date_in_format']` if provided,
    otherwise try to derive it from `timeoffdate` / `start_date` strings
    using a set of common date formats. Falls back to dateutil.parser if available.
    """
    start_date = dag_run.conf.get("start_date_in_format")
    if isinstance(start_date, dict) and all(k in start_date for k in ("year", "month", "day")):
        return start_date

    date_str = dag_run.conf.get("timeoffdate") or dag_run.conf.get("start_date") or dag_run.conf.get("date")
    if not date_str:
        raise KeyError("start_date_in_format not present and no 'timeoffdate'/'start_date' found in dag_run.conf")

    date_str = str(date_str).strip()

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y%m%d",
        "%m-%d-%Y",
        "%b %d, %Y",   # Jan 7, 2026
        "%B %d, %Y",   # January 7, 2026
        "%b %d %Y",    # Jan 7 2026
        "%B %d %Y",    # January 7 2026
    )

    for fmt in formats:
        try:
            parsed = dt.strptime(date_str, fmt)
            return {"year": parsed.strftime("%Y"), "month": parsed.strftime("%m"), "day": parsed.strftime("%d")}
        except Exception:
            continue

    # ISO parser as a last attempt
    try:
        parsed = dt.fromisoformat(date_str)
        return {"year": parsed.strftime("%Y"), "month": parsed.strftime("%m"), "day": parsed.strftime("%d")}
    except Exception:
        pass

    # dateutil if available for more flexible parsing
    try:
        from dateutil.parser import parse as _dateutil_parse
        parsed = _dateutil_parse(date_str)
        return {"year": parsed.strftime("%Y"), "month": parsed.strftime("%m"), "day": parsed.strftime("%d")}
    except Exception:
        pass

    raise ValueError(f"Could not parse date from '{date_str}' to derive start_date_in_format")

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

def get_timeoff_date(dag_run):
    sd = _get_start_date_in_format_from_conf(dag_run)
    return {
        "year": sd["year"],
        "month": sd["month"],
        "day": sd["day"]
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
def get_run_report_payload1():
    get_specific_report_details = rail.result('get_specific_report_details1')

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }   
    
def get_run_report_payload():
    get_specific_report_details = rail.result('get_specific_report_details')

    def get_specific_filter_uri(filter_name):
        return rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"], 'displayText', filter_name, 'uri')

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="ModifiedOnUtcDateRangeFilter"),
                        "value": None
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="ModifiedOnUtcDateRangeFilter"),
                        "value": str((now(config.timezone) - timedelta(days=1)).strftime('%m/%d/%Y'))
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="ModifiedOnUtcDateRangeFilter"),
                        "value": str(now(config.timezone).strftime('%m/%d/%Y'))
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
def get_create_timeoff_payload(dag_run, booking_type):
    if booking_type == 'F':
        relative_duration = "urn:replicon:time-off-relative-duration:full-day"
        get_start_timeoff_day = null
        timeoff_end = null
        start_specific_duration = null

    elif booking_type == 'H':
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
                "uri": dag_run.conf["useruri"],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": null,
                "name": dag_run.conf["timeofftype"]
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
            "comments": "ForceApproved - Replicon Integration",
            "customFieldValues": []
        }
}

def get_approve_timeoff_booking_payload(booking_type):
    return {
        "timeOffUri": rail.result(f"publish_timeoff_draft_for_user_type_{ booking_type }")["uri"],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "ForceApproved - Replicon Integration"
    }
    
def get_time_off_details_for_user_and_date_range_payload(dag_run):
    sd = _get_start_date_in_format_from_conf(dag_run)
    return {
        "userUri": dag_run.conf.get("useruri"),
        "dateRange": {
          "startDate": {
            "year": sd["year"],
            "month": sd["month"],
            "day": sd["day"]
          },
          "endDate": {
            "year": sd["year"],
            "month": sd["month"],
            "day": sd["day"]
          },
          "relativeDateRangeUri": null,
          "relativeDateRangeAsOfDate": null
        }
    }

def delete_timeoff_payload_1():
    result = rail.result("get_time_off_details_for_user_and_date_range_1")
    item = result[0] if isinstance(result, list) and len(result) > 0 else (result or {})
    return {
        "timeOffUri": item.get("uri")
    }
