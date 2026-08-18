"""
Request Payload Builder for iPipeline JIRA-Replicon Integration
Constructs API request payloads for various Replicon service calls
"""
from datetime import datetime
import itertools
import rail
from uuid import uuid4
import pendulum

null = None


def payload_to_get_all_replicon_projects():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code",
            "urn:replicon:project-list-column:status"
        ],
        "sort": [],
        "filterExpression": null
    }


def create_metadata_payload(dag_run, task_uri):
    """
    Build metadata array for time entry creation.

    Constructs the customMetadata section of time entry payload including
    task and comments, based on available data.

    Args:
        dag_run: Airflow DAG run object containing entry data and URIs
        task_uri: Optional Replicon URI for the project task

    Returns:
        List[Dict[str, Any]]: Array of metadata objects for time entry
    """
    metadata = []

    project_details = rail.result('get_all_project_details')['projectDetails']
    
    metadata.append({
        "keyUri": "urn:replicon:time-entry-metadata-key:is-billable",
        "value": {
          "bool": rail.result('get_task_and_project_billable_details')['is_billable'],
        }
      })

    metadata.append({
        'keyUri': 'urn:replicon:time-entry-metadata-key:task',
        'value': {
            'uri': task_uri
        }
    })

    metadata.append({
        'keyUri': 'urn:replicon:time-entry-metadata-key:project',
        'value': {
            'uri': project_details['uri']
        }
    })

    if dag_run.conf['time_entry_comment']:
        metadata.append({
            'keyUri': 'urn:replicon:time-entry-metadata-key:comments',
            'value': {
                'text': dag_run.conf['time_entry_comment']
            }
        })

    return metadata


def get_extension_field_payload(dag_run, oef_mapper):
    payload = []

    for k, v in oef_mapper.items():
        if dag_run.conf.get(v):
            payload.append({
                "definition": {
                    "name": k
                },
                "textValue": dag_run.conf.get(v)
            })

    return payload


def put_time_entry_payload(dag_run, ENTRY_DATE_FORMAT, oef_mapper):
    """
    Generate complete payload for creating or updating a time entry in Replicon.

    Builds the full PutTimeEntryRevisionGroup API payload including user context,
    entry date, time allocation data, project metadata,
    and Object Extension Field values.

    Args:
        dag_run: Airflow DAG run object containing entry data and user context
        task_uri: Optional Replicon URI for the project task

    Returns:
        Dict[str, Any]: Complete API payload for time entry creation/update
    """
    task_uri = rail.find_first_by_attr_and_get_attr(rail.result(
        'get_required_tasks_for_project'), 'full_task_name', dag_run.conf['task_type'], 'uri', '')
    parsed_date = datetime.strptime(
        dag_run.conf['time_entry_date'], ENTRY_DATE_FORMAT)

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
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": {
                    "hours": 0,
                    "minutes": 0,
                    "seconds": int(float(dag_run.conf["hours"])*3600) if dag_run.conf["hours"] else 0,
                    "milliseconds": 0,
                    "microseconds": 0
                },
                "timePair": null
            },
            "customMetadata": create_metadata_payload(dag_run, task_uri),
            "extensionFieldValues": get_extension_field_payload(dag_run, oef_mapper),
        },
        "unitOfWorkId": str(uuid4())
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
    entry_date = rail.parse_date(
        dag_run.conf['time_entry_date'], ENTRY_DATE_FORMAT)

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


def get_process_each_entry_conf(item):
    conf = {
        **item,
        'user_uri': rail.result('get_details_of_user_in_replicon')['uri'],
        'user_loginname': rail.result('get_user_email_in_jira'),
        'log': rail.result('create_user_time_entries_log')
    }

    return conf
