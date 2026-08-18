from uuid import uuid4
from functools import lru_cache
from datetime import datetime
import rail

null = None

def get_location_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def report_config(dag_run):
    entrydatefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri')
    locationfilter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "CurrentLocationFilter", 'uri')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")["uri"],
                "filterValues": [
                    {
                        "reportFilterUri": locationfilter,
                        "value": dag_run.conf["input_data"]["org_uri"].split(':')[-1]
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": null
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": dag_run.conf["input_data"]["timesheet_start_date"]
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": dag_run.conf["input_data"]["timesheet_end_date"]
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_user_data_payload(dag_run):
    
    return {
        "users": [
            {"uri": dag_run.conf["input_data"]["user_uri"], "loginName": null, "employeeId": null, "parameterCorrelationId": null}
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_timesheet_details_payload(item, dag_run, entry_date_format):
    user_uri = dag_run.conf["input_data"]["user_uri"]
    date_str = item["timesheet_start_date"]
    
    date_obj = rail.parse_date(date_str, entry_date_format)

    return {
        "userUri": user_uri,
        "date": date_obj,
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary",
    }

def get_interval_from_hours(hours):
    """
    Convert hours string to Replicon interval format.
    
    Transforms decimal hours (e.g., '8.5') into Replicon's interval structure
    with hours, minutes, seconds, milliseconds, and microseconds components.
    
    Args:
        hours: String representation of decimal hours
        
    Returns:
        Dict[str, int]: Interval object with time components in seconds
    """
    seconds = int(float(hours) * 3600)
    return {
        "hours": 0,
        "minutes": 0,
        "seconds": seconds,
        "milliseconds": 0,
        "microseconds": 0
    }
def get_time_entries_for_user_date_range(dag_run, ENTRY_DATE_FORMAT):
    # Get data from dag_run.conf
    entry_date_str = dag_run.conf["input_data"]["entry_date"]
    user_uri = dag_run.conf["user_uri"]
    
    entry_date = rail.parse_date(entry_date_str, ENTRY_DATE_FORMAT)

    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "dateRange": {
            "startDate": entry_date,
            "endDate": entry_date,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_time_entry_details_payload(dag_run):
    return {
        "timeEntryRevisionGroups": [{
            "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:time-entry-revision-group:{dag_run.conf['input_data']['entry_id']}",
            "parameterCorrelationId": null
        }]
    }

@lru_cache(maxsize=8)
def get_date(entry_date, ENTRY_DATE_FORMAT):
    """
    Parse entry date string to datetime object with caching.
    
    Converts date string in configured format to datetime object for API calls.
    Uses LRU cache to avoid repeated parsing of the same dates.
    
    Args:
        entry_date: Date string in format specified by config.ENTRY_DATE_FORMAT
        
    Returns:
        datetime: Parsed datetime object
    """
    return datetime.strptime(entry_date, ENTRY_DATE_FORMAT) if isinstance(entry_date, str) else entry_date

def reopen_timesheet_payload(item):
    return {
        "timesheetUri": item["timesheet_uri"],
        "unitOfWorkId": str(uuid4()),
        "comments": "Timesheet is reopened by the Integration (Guessing Hours Update)"
    }

def submit_timesheet_payload(item):
    return {
        "timesheetUri": item["timesheet_uri"],
        "unitOfWorkId": str(uuid4()),
        "comments": "Timesheet is submitted by the Integration (Guessing Hours Update)",
        "changeReason": null
    }

def force_approve_timesheet_payload(item):
    return {
        "timesheetUri": item["timesheet_uri"],
        "unitOfWorkId": str(uuid4()),
        "comments": "Timesheet is force-approved by the Integration (Guessing Hours Update)"
    }

def put_time_entry_payload(dag_run, ENTRY_DATE_FORMAT):
    time_entry_details = rail.result("get_time_entry_details")
    
    if not time_entry_details:
        raise ValueError("No time entry details found")
    
    # Get user_uri and entry_date from dag_run.conf
    user_uri = dag_run.conf["user_uri"]
    entry_date_str = dag_run.conf["input_data"]["entry_date"]
    
    # Parse the entry date
    parsed_date = get_date(entry_date_str, ENTRY_DATE_FORMAT)
    
    return {
        "timeEntryRevisionGroup": {
            "target": {
                "uri": time_entry_details["entry_uri"]
            },
            "user": {
                "uri": user_uri
            },
            "entryDate": {
                "year": parsed_date.year,
                "month": parsed_date.month,
                "day": parsed_date.day
            },
            "timeAllocationTypeUris": time_entry_details.get('time_allocation_types', []),
            "interval": {
                "hours": get_interval_from_hours("0"),
                "timePair": null
            },
            "customMetadata": time_entry_details.get('custom_metadata', []),
            "extensionFieldValues": time_entry_details.get('extension_fields', [])
        },
        "unitOfWorkId": str(uuid4())
    }