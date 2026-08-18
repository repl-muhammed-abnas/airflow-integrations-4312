"""Request payload builders for Eisner Amper TimeOff Import API calls."""

from datetime import datetime
from functools import lru_cache
from uuid import uuid4
import rail

null = None
FEED_ENTRYDATE_DATE_FORMAT = "%d/%m/%Y"

def get_user_data_payload(dag_run):
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

def get_time_entries_for_user_date_range(dag_run, ENTRY_DATE_FORMAT):
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
    entry_date = rail.parse_date(dag_run.conf['input_data']['start_date'], ENTRY_DATE_FORMAT)

    return {
        "user": {
            "uri": dag_run.conf['user_uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "dateRange": {
            "startDate": {
                "year": entry_date["year"],
                "month": entry_date["month"],
                "day": entry_date["day"]
            },
            "endDate": {
                "year": entry_date["year"],
                "month": entry_date["month"],
                "day": entry_date["day"]
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
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

def create_metadata_payload(dag_run, task_uri):
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

    metadata.append({
        'keyUri': 'urn:replicon:time-entry-metadata-key:task',
        'value': {
            'uri': task_uri
        }
    })

    metadata.append({
        'keyUri': 'urn:replicon:time-entry-metadata-key:project',
        'value': {
            'uri': rail.result('get_all_project_details')['projectDetails']['uri']
        }
    })

    if dag_run.conf['input_data']['booking_reference_id']:
        metadata.append({
            'keyUri': 'urn:replicon:time-entry-metadata-key:external-id',
            'value': {
                'text': dag_run.conf['input_data']['booking_reference_id']
            }
        })

    return metadata


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

def put_time_entry_payload(dag_run, ENTRY_DATE_FORMAT):
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
    # For time off entries from Workday, always use "Default Task"
    task_uri = rail.result('get_required_tasks_for_project')
    time_entries = dag_run.conf['input_data']
    parsed_date = get_date(time_entries['start_date'], ENTRY_DATE_FORMAT)
    # Use enriched data from parent DAG instead of making API call
    target_uri = dag_run.conf['input_data']['existing_entry_uri'] if 'existing_entry_uri' in dag_run.conf['input_data'] else None

    return {
        "timeEntryRevisionGroup": {
            "target": {
                "uri": target_uri
            } if target_uri else null,
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
            "customMetadata": create_metadata_payload(dag_run, task_uri),
            # TODO: Update Object Extension Field URIs as needed
            "extensionFieldValues": [
                {
                    "tag": {
                        "uri": dag_run.conf['timesheet_work_location_uri']
                    },
                    "definition": {
                        "uri": dag_run.conf['work_location_oef_uri']
                    }
                }
            ] if dag_run.conf['work_location_oef_uri'] and dag_run.conf['timesheet_work_location_uri'] else []
        },
        "unitOfWorkId": str(uuid4())
    }
