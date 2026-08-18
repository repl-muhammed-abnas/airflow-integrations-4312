"""Request payload builders for T-Systems Time Import API calls."""

from typing import Dict, Any, Optional, List
from datetime import datetime
from functools import lru_cache
from uuid import uuid4
from tsystems.time_import import config
import rail

null = None
FEED_ENTRYDATE_DATE_FORMAT = "%d/%m/%Y"

def get_user_data_payload(dag_run) -> Dict[str, Any]:
    """
    Build payload for retrieving user details from Replicon.
    
    Creates request payload for BulkGetUsers3 API call to fetch user profile,
    timesheet template, and assigned activities by employee ID.
    
    Args:
        dag_run: Airflow DAG run object containing employee_id in configuration
        
    Returns:
        Dict[str, Any]: API request payload for user details lookup
    """
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['employee_id'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_time_entries_for_user_date_range(user_uri: str, entry_date: Dict[str, int]) -> Dict[str, Any]:
    """
    Generate payload to retrieve existing time entries for a user on a specific date.
    
    Creates request payload for GetTimeEntryRevisionGroupsForUserAndDateRange API
    to find existing time entries that may need deletion before adding new ones.
    
    Args:
        user_uri: Replicon URI identifying the user
        entry_date: Dictionary with 'year', 'month', 'day' keys for the target date
        
    Returns:
        Dict[str, Any]: API request payload for time entry lookup
    """
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

def get_interval_from_hours(hours: str) -> Dict[str, int]:
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


def get_timepair_from_inout(intime: str, outtime: str) -> Dict[str, Dict[str, int]]:
    """
    Convert in-time and out-time strings to Replicon timePair format.
    
    Transforms 'HH:MM' formatted time strings into Replicon's time pair structure
    with separate startTime and endTime objects containing hour, minute, second.
    
    Args:
        intime: Start time in 'HH:MM' format (e.g., '09:00')
        outtime: End time in 'HH:MM' format (e.g., '17:30')
        
    Returns:
        Dict[str, Dict[str, int]]: TimePair object with startTime and endTime
    """
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


def create_metadata_payload(dag_run, task_uri: Optional[str]) -> List[Dict[str, Any]]:
    """
    Build metadata array for time entry creation.
    
    Constructs the customMetadata section of time entry payload including
    task, activity, billing rate, and comments based on available data.
    
    Args:
        dag_run: Airflow DAG run object containing entry data and URIs
        task_uri: Optional Replicon URI for the project task
        
    Returns:
        List[Dict[str, Any]]: Array of metadata objects for time entry
    """
    metadata = []

    # Add task metadata
    if task_uri:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:task",
            "value": {
                "uri": task_uri
            }
        })

    # Add task metadata
    if dag_run.conf['activity_uri']:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:activity",
            "value": {
                "uri": dag_run.conf['activity_uri']
            }
        })

    if dag_run.conf["billing_rate_uri"]:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:billing-rate",
            "value": {
                "uri": dag_run.conf["billing_rate_uri"]
            }
        })
    if dag_run.conf['entry_data']["comments"]:
        metadata.append({
            "keyUri": "urn:replicon:time-entry-metadata-key:comments",
            "value": {
                "text": dag_run.conf['entry_data']["comments"]
            }
        })

    return metadata


@lru_cache(maxsize=8)
def get_date(entry_date: str) -> datetime:
    """
    Parse entry date string to datetime object with caching.
    
    Converts date string in configured format to datetime object for API calls.
    Uses LRU cache to avoid repeated parsing of the same dates.
    
    Args:
        entry_date: Date string in format specified by config.entry_dateformat
        
    Returns:
        datetime: Parsed datetime object
    """
    return datetime.strptime(entry_date, config.entry_dateformat) if isinstance(entry_date, str) else entry_date

def put_time_entry_payload(dag_run, task_uri: Optional[str]) -> Dict[str, Any]:
    """
    Generate complete payload for creating or updating a time entry in Replicon.
    
    Builds the full PutTimeEntryRevisionGroup API payload including user context,
    entry date, time allocation data (hours or in/out times), project metadata,
    and Object Extension Field values based on timesheet template type.
    
    Args:
        dag_run: Airflow DAG run object containing entry data and user context
        task_uri: Optional Replicon URI for the project task
        
    Returns:
        Dict[str, Any]: Complete API payload for time entry creation/update
    """
    time_entries = dag_run.conf['entry_data']
    parsed_date = get_date(time_entries["entry_date"])
    ts_type = dag_run.conf['user_ts_type']

    return {
        "timeEntryRevisionGroup": {
            "target": null,
            "user": {
                "uri": dag_run.conf["user_uri"]
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
                "hours": get_interval_from_hours(time_entries["hours"]) if bool(float(time_entries["hours"])) else null,
                "timePair": null
            },
            "customMetadata": create_metadata_payload(dag_run, task_uri) if task_uri else null,
            "extensionFieldValues": [],
        },
        "unitOfWorkId": str(uuid4())
    }

def put_inout_entry_payload(dag_run) -> Dict[str, Any]:
    """
    Generate payload for creating attendance-based time entries with in/out times.
    
    Creates PutTimeEntryRevisionGroup API payload specifically for attendance time
    allocation using in-time and out-time pairs. This is used for timesheet templates
    that support punch-in/punch-out functionality with Object Extension Fields.
    
    Args:
        dag_run: Airflow DAG run object containing entry date, user context,
                 and OEF configuration from prerequisite tasks
        
    Returns:
        Dict[str, Any]: API payload for attendance time entry with timePair interval
                        and OEF values for worktype classification
    """
    parsed_date = get_date(dag_run.conf["entry_date"])
    ts_type = dag_run.conf['user_ts_type']

    return {
        "timeEntryRevisionGroup": {
            "target": null,
            "user": {
                "uri": dag_run.conf["user_uri"]
            },
            "entryDate": {
                "year": parsed_date.year,
                "month": parsed_date.month,
                "day": parsed_date.day
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance"
            ],
            "interval": {
                "hours": null,
                "timePair": get_timepair_from_inout(dag_run.conf['in_time'], dag_run.conf['out_time']) if dag_run.conf and (
                    dag_run.conf['in_time'] and dag_run.conf['out_time'] and \
                    ts_type in [
                        config.timesheet_inout_dist,
                        config.timesheet_inout_dist_with_oef
                    ]) else null
            },
            "customMetadata": null,
            "extensionFieldValues": [{
                "definition": {
                    "uri": rail.result('get_oef_and_tags')['oef_uri']
                },
                "numericValue": null,
                "textValue": null,
                "tag": {
                    "uri": rail.result('get_oef_and_tags')['oef_tag_uri']
                },
                "jsonValue": null
                }] if ts_type == config.timesheet_inout_dist_with_oef and rail.result('get_oef_and_tags') else [],
        },
        "unitOfWorkId": str(uuid4())
    }

