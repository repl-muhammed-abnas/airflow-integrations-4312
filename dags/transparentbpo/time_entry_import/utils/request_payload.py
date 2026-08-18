from typing import Dict, Any,List
from datetime import datetime
from functools import lru_cache
from uuid import uuid4
from transparentbpo.time_entry_import import config
import rail

null = None

def get_user_data_payload(employee_id) -> Dict[str, Any]:
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": employee_id,
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_interval_from_hours(seconds: float) -> Dict[str, int]:
    total_seconds = int(seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    return {
        "hours": hours,
        "minutes": minutes,
        "seconds": secs,
        "milliseconds": 0,
        "microseconds": 0
    }

def get_timepair_from_inout(intime: str, outtime: str) -> Dict[str, Dict[str, int]]:
    def parse_time(timestr):
        h, m = (timestr or "0:0").split(":")
        return {
            "hour": int(h),
            "minute": int(m),
            "second": 0
        }
    return {
        "startTime": parse_time(intime),
        "endTime": parse_time(outtime)
    }

def create_metadata_payload(time_entry) -> List[Dict[str, Any]]:
    metadata = []

    task_uri = time_entry.get("task_uri")
    if task_uri:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:task",
            "value": {
                "uri": task_uri
            }
        })

    assigned_activities = rail.get_current_context()["dag_run"].conf.get("activities", [])
    activity_uri = rail.find_first_by_attr_and_get_attr(
        assigned_activities, "name", time_entry["activity"], "uri"
    )
    if activity_uri:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:activity",
            "value": {
                "uri": activity_uri
            }
        })

    return metadata

@lru_cache(maxsize=128)
def get_date(entry_date: str) -> datetime:
    return datetime.strptime(entry_date, config.entry_dateformat) if isinstance(entry_date, str) else entry_date

def put_time_entry_payload(dag_run, batch_size: int = 50):
    """
    Accepts a list of dag_run objects and yields batched payloads of up to 50 entries.
    """
    user_uri = dag_run.conf["user_uri"]
    user_time_entry_records = rail.result("get_aggregate_seconds_for_activity")
    def build_revision_group(time_entry):
        parsed_date = get_date(time_entry["work_date"])
        return {
            "target": None,
            "user": {
                "uri": user_uri
            },
            "entryDate": {
                "year": parsed_date.year,
                "month": parsed_date.month,
                "day": parsed_date.day
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": get_interval_from_hours(time_entry["total_seconds"]),
                "timePair": None
            },
            "customMetadata": create_metadata_payload(time_entry),
            "extensionFieldValues": []
        }

    batches = [user_time_entry_records[i:i + batch_size] for i in range(0, len(user_time_entry_records), batch_size)]

    payloads = []
    for batch in batches:
        payloads.append({
            "timeEntryRevisionGroups": [build_revision_group(time_entry) for time_entry in batch],
            "bulkPutTimeEntryRevisionGroupBehaviour": {
                "bulkPutTimeEntryRevisionGroupBehaviourErrorHandlingOptionUri":
                    "urn:replicon:bulk-put-time-entry-revision-group-behaviour-error-handling-option:fault-and-rollback-on-error"
            },
            "unitOfWorkId": str(uuid4())
        })

    return payloads

def parse_clock_time(timestr):
    if not isinstance(timestr, str) or not timestr.strip():
        raise ValueError(f"Invalid time format: {timestr!r}; expected HH:MM, HH:MM:SS, or HH:MM[:SS] AM/PM")

    value = timestr.strip()
    ampm = None
    parts = value.split()
    if len(parts) == 2:
        value, ampm = parts[0], parts[1].lower()
        if ampm not in ("am", "pm"):
            raise ValueError(f"Invalid AM/PM marker in time: {timestr!r}")
    elif len(parts) > 2:
        raise ValueError(f"Invalid time format: {timestr!r}")

    segments = value.split(":")
    if len(segments) < 2:
        raise ValueError(f"Invalid time format: {timestr!r}")

    hour = int(segments[0])
    minute = int(segments[1])
    second = int(segments[2]) if len(segments) == 3 else 0

    if ampm:
        if hour < 1 or hour > 12:
            raise ValueError(f"Hour must be 1-12 when AM/PM given, got {hour}")
        if ampm == "am":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
    else:
        if hour < 0 or hour > 23:
            raise ValueError(f"Hour must be 0-23 when no AM/PM given, got {hour}")

    if minute < 0 or minute > 59:
        raise ValueError(f"Minute must be 0-59, got {minute}")
    
    if second < 0 or second > 59:
        raise ValueError(f"Second must be 0-59, got {second}")

    return str(hour), str(minute), str(second)

def get_time_entries_for_user_date_range(user_uri, work_date):
    year, month, day = work_date["year"], work_date["month"], work_date["day"]

    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "dateRange": {
            "startDate": {
                "year": year,
                "month": month,
                "day": day
            },
            "endDate": {
                "year": year,
                "month": month,
                "day": day
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

def get_bulk_put_time_punch_payload(batch_size: int = 50):
    dag_run = rail.get_current_context()["dag_run"]
    aggregated_entries = rail.result("get_aggregate_seconds_for_activity")
    all_time_punches = []
    for entry in aggregated_entries:
        for punch_record in entry["punch_records"]:
            all_time_punches.extend(build_bulk_put_time_punch_payload(dag_run, punch_record))

    batches = [all_time_punches[i:i + batch_size] for i in range(0, len(all_time_punches), batch_size)]

    return [
        {
            "timePunches": batch,
            "bulkPutTimePunchBehaviour": {
                "bulkPutTimePunchBehaviourErrorHandlingOptionUri":
                    "urn:replicon:bulk-put-time-punch-behaviour-error-handling-option:fault-and-rollback-on-error"
            },
            "unitOfWorkId": str(uuid4())
        }
        for batch in batches
    ]

def build_bulk_put_time_punch_payload(dag_run, item):
    null = None
    
    entry_date_dict = rail.parse_date(dag_run.conf["work_date"], "%m/%d/%Y")
    break_type = rail.find_first_by_attr_and_get_attr(
        dag_run.conf["break_types"], "displayText", item["timesheet_category"], "uri"
    )

    punch_in_h, punch_in_m, punch_in_s = parse_clock_time(item.get("punch_start_time", ""))
    punch_in_datetime = {
        "year": str(entry_date_dict["year"]),
        "month": str(entry_date_dict["month"]),
        "day": str(entry_date_dict["day"]),
        "hour": str(punch_in_h),
        "minute": str(punch_in_m),
        "second": str(punch_in_s),
        "timeZoneUri": null
    }

    punch_out_h, punch_out_m, punch_out_s = parse_clock_time(item.get("punch_end_time", ""))
    punch_out_datetime = {
        "year": str(entry_date_dict["year"]),
        "month": str(entry_date_dict["month"]),
        "day": str(entry_date_dict["day"]),
        "hour": str(punch_out_h),
        "minute": str(punch_out_m),
        "second": str(punch_out_s),
        "timeZoneUri": null
    }

    time_punches = [
        {
            "timePunch": {
                "target": {"parameterCorrelationId": null, "uri": null, "slug": null},
                "user": {
                    "uri": dag_run.conf['user_uri'],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
                "punchTime": punch_in_datetime,
                "actionUri": "urn:replicon:time-punch-action:start-break" if break_type else (
                    "urn:replicon:time-punch-action:in"
                    if not item['is_transfer']
                    else "urn:replicon:time-punch-action:transfer"
                ),
                "punchInAttributes": null,
                "punchStartBreakAttributes": {
                    "breakType": {
                        "uri": break_type,
                        "name": null
                    }
                } if break_type else null,
                "extensionFieldValues": [],
                "rawTimePunchUri": null
            }
        }
    ]

    if item.get("punch_end_time") != item.get("next_record_start_time"):
        time_punches.append(
            {
                "timePunch": {
                    "target": {"parameterCorrelationId": null, "uri": null, "slug": null},
                    "user": {
                        "uri": dag_run.conf['user_uri'],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                    "punchTime": punch_out_datetime,
                    "actionUri": "urn:replicon:time-punch-action:out",
                    "punchStartBreakAttributes": null,
                    "extensionFieldValues": [],
                    "rawTimePunchUri": null
                }
            }
        )
    return time_punches

def get_time_punch_details(user_uri, work_date):
    year, month, day = work_date["year"], work_date["month"], work_date["day"]
    return {
  "userUris": [
    user_uri
  ],
  "dateRange": {
    "startDate": {
                "year": year,
                "month": month,
                "day": day
            },
            "endDate": {
                "year": year,
                "month": month,
                "day": day
            },
    "relativeDateRangeUri": null,
    "relativeDateRangeAsOfDate": null
  },
  "timePunchTimeSegmentDateRangeFilterOption": "urn:replicon:time-punch-time-segment-date-range-filter-option:punch-user-time-zone"
}

def get_project_report_params():
    project_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result("get_report_details")[
        'filterConfiguration']['enabledFilters'], "displayText", "ProjectFilter", 'uri')

    filter_values = []
    for item in rail.result("get_all_project_details"):
        if not item:
            continue
        filter_values.append({
            "reportFilterUri": project_filter_uri,
            "value": item['uri'].split(":")[-1]
        })
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
