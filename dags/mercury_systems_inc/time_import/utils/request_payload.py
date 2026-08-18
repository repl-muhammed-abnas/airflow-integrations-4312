from datetime import datetime
from functools import lru_cache
from uuid import uuid4
import rail

null = None

SQL_DATEFORMAT = "%Y-%m-%d"


def get_timesheet_for_date(user_uri, entry_date):
    """Generate payload to get or create timesheet for a specific date"""
    return {
        "userUri": user_uri,
        "date": entry_date,
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }


def get_time_entries_for_user_date_range(user_uri, entry_date):
    """Generate payload to get time entries for a user within a date range"""
    year, month, day = entry_date["year"], entry_date["month"], entry_date["day"]

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


def get_interval_from_hours(hours):
    """Convert hours to interval format"""
    seconds = int(float(hours) * 3600)
    return {
        "hours": 0,
        "minutes": 0,
        "seconds": seconds,
        "milliseconds": 0,
        "microseconds": 0
    }


def create_metadata_payload(task_uri, is_billable=True):
    """Create metadata for time entry"""
    metadata = []

    # Add task metadata
    metadata.append({
        "keyUri": "urn:replicon:time-entry-metadata-key:task",
        "value": {
            "uri": task_uri
        }
    })

    # Add billable flag
    metadata.append({
        "keyUri": "urn:replicon:time-entry-metadata-key:is-billable",
        "value": {
            "bool": is_billable
        }
    })
    # Add timestamp for tracking
    metadata.append({
        "keyUri": "urn:replicon:widget-ui-metadata-key:initial-row-number",
        "value": {
            "number": str(round((datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)).total_seconds()))
        }
    })

    return metadata


@lru_cache(maxsize=128)
def get_date(entry_date):
    return datetime.strptime(entry_date, SQL_DATEFORMAT) if isinstance(entry_date, str) else entry_date


def put_time_entry_payload(time_entries):
    """Generate payload for creating/updating time entry"""
    parsed_date = get_date(time_entries["entry_date"])
    target = {"uri": time_entries["time_entry_uri"]} if time_entries.get(
        "time_entry_uri") else null

    return {
        "timeEntryRevisionGroup": {
            "target": target,
            "user": {
                "uri": time_entries["user_uri"]
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
                "hours": get_interval_from_hours(time_entries["total_hours"]),
                "timePair": null
            },
            "customMetadata": create_metadata_payload(time_entries["task_uri"])
        },
        "unitOfWorkId": str(uuid4())
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
