from typing import Dict, Any, List
from datetime import datetime
from uuid import uuid4
import rail

null = None

def get_user_data_payload(therapist) -> Dict[str, Any]:
    return {
        "users": [
            {
            "uri": null,
            "loginName": therapist,
            "employeeId": null,
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_interval_from_hours(seconds: str) -> Dict[str, int]:
    return {
        "hours": "0",
        "minutes": "0",
        "seconds": int(seconds),
        "milliseconds": "0",
        "microseconds": "0"
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

def create_metadata_payload(dag_run) -> List[Dict[str, Any]]:
    metadata = []

    if dag_run.conf["task_uri"]:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:task",
            "value": {
                "uri": dag_run.conf["task_uri"]
            }
        })

    if dag_run.conf['activity_uri']:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:activity",
            "value": {
                "uri": dag_run.conf['activity_uri']
            }
        })

    return metadata

def get_date(entry_date: str, entry_dateformat: str) -> datetime:
    return datetime.strptime(entry_date, entry_dateformat) if isinstance(entry_date, str) else entry_date

def put_time_entry_payload(dag_run, entry_dateformat):
    parsed_date = get_date(dag_run.conf["date_of_service"], entry_dateformat)

    service_name_oef = rail.result("get_tag_details_for_service_name_oef")
    type_billing_oef = rail.result("get_tag_details_for_type_billing_oef")
    student_count_oef_uri = rail.find_first_by_attr_and_get_attr(dag_run.conf.get("object_extension_fields"),"name","Student Count","uri")  
    extension_field_values = [
        {
            "definition": {
                "uri": service_name_oef["oef_uri"]
            },
            "tag": {
                "uri": service_name_oef["oef_value_uri"],
                "slug": service_name_oef["oef_value"],
                "tagName": null
                },
            "numericValue": null,
            "textValue": null,
            "fileValue": null,
            "jsonValue": null
        },

        {
            "definition": {
                "uri": student_count_oef_uri
            },
            "numericValue": int(dag_run.conf.get("num_students")),
            "textValue": None,
            "tag": None,
            "jsonValue": None
        },
        {
            "definition": {
                "uri": type_billing_oef["oef_uri"]
            },
            "tag": {
                "uri": type_billing_oef["oef_value_uri"],
                "slug": type_billing_oef["oef_value"],
                "tagName": null
            },
            "numericValue": null,
            "textValue": null,
            "fileValue": null,
            "jsonValue": null
        }
    ]

    return {
        "timeEntryRevisionGroup": {
            "target": {
            "uri": null,
            "parameterCorrelationId": null
            },
            "user": {
                "uri": dag_run.conf["user_uri"]
            },
            "interval": {
                "hours": get_interval_from_hours(
                    float(dag_run.conf["hours"])*60*60
                ),
                "timePair": None
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "entryDate": {
                "year": parsed_date.year,
                "month": parsed_date.month,
                "day": parsed_date.day
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:activity",
                    "value": {
                        "uri": rail.result("get_activity_assigned_to_user")
                    }
                },
                {
                    "keyUri": "urn:replicon:widget-ui-metadata-key:row-number",
                    "value": {
                        "number": 1
                    }
                }
            ],
            "extensionFieldValues": extension_field_values
        },
        "unitOfWorkId": str(uuid4())
    }
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

    return str(hour), str(minute)

def get_time_entries_for_user_date_range(user_uri, work_date):
    """Generate payload to get time entries for a user within a date range"""
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
