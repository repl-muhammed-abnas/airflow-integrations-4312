"""
JIRA Time Sync Integration - Payload Builders
==============================================

This module contains functions to build API request payloads
for Replicon Polaris and JIRA comment APIs.

NOTE: Costpoint payload builders are not part of the current release —
see ../backup/costpoint_integration_backup.py.
"""

from typing import Dict, Any, Optional
from uuid import uuid4

from jira_time_sync_cp_polaris.utils.transformers import (
    jira_date_to_replicon,
    seconds_to_replicon_interval,
    build_replicon_comments,
)


def build_jira_comment(heading: str, date: str, hours, user: str, project_path: str, info: str, success: bool = True, account_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Build a uniform JIRA comment for all outcomes (create, update, delete, failure).

    Structure (green panel for success, red for failure):
        [Heading]
        Date: {date}  |  Hours: {hours}  |  User: @mention (or email fallback)
        Project: {project_path}
        Info: {info}

    Args:
        heading: e.g. "Time Entry Created in Polaris"
        date: Time entry date string (YYYY-MM-DD)
        hours: Decimal hours logged
        user: Worklog author email address (used as fallback when account_id absent)
        project_path: Full project path string (e.g. "MyProject > Task1 > Task2")
        info: Human-readable outcome — success detail or failure reason
        success: True for green success panel, False for red error panel
        account_id: JIRA accountId — when provided, renders as a @mention tag
                    that notifies the user; falls back to plain email text if absent

    Returns:
        JIRA REST API v3 comment payload with ADF body
    """
    panel_type = 'success' if success else 'error'

    if account_id:
        user_node = {
            'type': 'mention',
            'attrs': {'id': account_id, 'text': f'@{user}'}
        }
    else:
        user_node = {'type': 'text', 'text': str(user)}

    panel_content = [
        {
            'type': 'heading',
            'attrs': {'level': 3},
            'content': [{'type': 'text', 'text': heading}]
        },
        {
            'type': 'paragraph',
            'content': [
                {'type': 'text', 'text': 'Date: ', 'marks': [{'type': 'strong'}]},
                {'type': 'text', 'text': str(date)},
                {'type': 'text', 'text': '  |  '},
                {'type': 'text', 'text': 'Hours: ', 'marks': [{'type': 'strong'}]},
                {'type': 'text', 'text': str(hours)},
                {'type': 'text', 'text': '  |  '},
                {'type': 'text', 'text': 'User: ', 'marks': [{'type': 'strong'}]},
                user_node
            ]
        }
    ]

    if project_path:
        panel_content.append({
            'type': 'paragraph',
            'content': [
                {'type': 'text', 'text': 'Project: ', 'marks': [{'type': 'strong'}]},
                {'type': 'text', 'text': project_path}
            ]
        })

    panel_content.append({
        'type': 'paragraph',
        'content': [
            {'type': 'text', 'text': 'Info: ', 'marks': [{'type': 'strong'}]},
            {'type': 'text', 'text': info}
        ]
    })

    return {
        'body': {
            'type': 'doc',
            'version': 1,
            'content': [
                {
                    'type': 'panel',
                    'attrs': {'panelType': panel_type},
                    'content': panel_content
                }
            ]
        }
    }


def build_replicon_user_search_payload(email: str) -> Dict[str, Any]:
    """
    Build Replicon API payload to look up a user by exact email address
    via UserListService1.svc/GetData.

    Uses urn:replicon:user-list-filter:email-address with an equal operator
    so the API returns only users whose email-address field exactly matches —
    no login-name or display-name bleed-through from a text-search.

    Args:
        email: JIRA worklog author's email address

    Returns:
        Replicon UserListService1.svc/GetData request payload
    """
    return {
        'page': '1',
        'pagesize': '10',
        'columnUris': [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:enabled',
            'urn:replicon:user-list-column:login-name',
            'urn:replicon:user-list-column:email-address'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'leftExpression': None,
                'operatorUri': None,
                'rightExpression': None,
                'value': None,
                'filterDefinitionUri': 'urn:replicon:user-list-filter:email-address'
            },
            'operatorUri': 'urn:replicon:filter-operator:equal',
            'rightExpression': {
                'leftExpression': None,
                'operatorUri': None,
                'rightExpression': None,
                'value': {
                    'uri': None,
                    'uris': [],
                    'bool': None,
                    'date': None,
                    'money': None,
                    'number': None,
                    'text': email,
                    'time': None,
                    'calendarDayDurationValue': None,
                    'workdayDurationValue': None,
                    'dateRange': None,
                    'dateTimeUtc': None
                },
                'filterDefinitionUri': None
            },
            'value': None,
            'filterDefinitionUri': None
        }
    }


def build_replicon_timesheet_lookup_payload(user_uri: str, date_string: str) -> Dict[str, Any]:
    """
    Build Replicon API payload to get or create timesheet for date.

    Args:
        user_uri: Replicon user URI
        date_string: Date string from JIRA (will be converted)

    Returns:
        Replicon GetTimesheetDetailsForDate request payload
    """
    date_parts = date_string.split("-")

    date_obj = {
        'year': int(date_parts[0]),
        'month': int(date_parts[1]),
        'day': int(date_parts[2])
    }

    return {
        'userUri': user_uri,
        'date': date_obj,
        'timesheetGetOptionUri': 'urn:replicon:timesheet-get-option:create-timesheet-if-necessary'
    }


def build_replicon_project_by_name_payload(project_name: str) -> Dict[str, Any]:
    """
    Build Replicon API payload to lookup project by name.

    Args:
        project_name: Project name (first segment of JIRA path field)

    Returns:
        Replicon BulkGetProjectDetails3 request payload
    """
    return {
        'projects': [
            {'name': project_name}
        ]
    }


def build_replicon_activity_search_payload(activity_name: str) -> Dict[str, Any]:
    """
    Build Replicon API payload to search for an activity by name via
    ActivityListService1.svc/GetData. Activities are tenant-specific custom
    values (like tasks/projects), so this resolves the activity's URI by
    name rather than relying on a fixed/static URN.

    Args:
        activity_name: Activity name to search for, e.g. "Work From Home"

    Returns:
        Replicon ActivityListService1.svc/GetData request payload
    """
    return {
        'page': '1',
        'pagesize': '10',
        'columnUris': [
            'urn:replicon:activity-list-column:activity',
            'urn:replicon:activity-list-column:code'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'leftExpression': None,
                'operatorUri': None,
                'rightExpression': None,
                'value': None,
                'filterDefinitionUri': 'urn:replicon:activity-list-filter:text'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'leftExpression': None,
                'operatorUri': None,
                'rightExpression': None,
                'value': {
                    'uri': None,
                    'uris': [],
                    'bool': None,
                    'date': None,
                    'money': None,
                    'number': None,
                    'text': activity_name,
                    'time': None,
                    'calendarDayDurationValue': None,
                    'workdayDurationValue': None,
                    'dateRange': None,
                    'dateTimeUtc': None
                },
                'filterDefinitionUri': None
            },
            'value': None,
            'filterDefinitionUri': None
        }
    }


def build_replicon_recalculate_payload(timesheet_uri: str) -> Dict[str, Any]:
    """
    Build Replicon API payload to enqueue timesheet recalculation.

    Args:
        timesheet_uri: Replicon timesheet URI

    Returns:
        Replicon EnqueueRecalculateScriptData request payload
    """
    return {
        'timesheet': {
            'uri': timesheet_uri,
            'user': None,
            'date': None
        }
    }


def build_replicon_task_search_payload(project_uri: str) -> Dict[str, Any]:
    """
    Build Replicon TaskListService1.svc/GetData payload to retrieve all tasks
    for a project with their full hierarchical path.

    Fetches task name/URI (cell 0), full-path (cell 1), and enabled flag (cell 2).
    The full-path column returns a cellCollection list of path node objects,
    enabling dynamic resolution of any task depth without level-by-level calls.

    Args:
        project_uri: Replicon project URI

    Returns:
        Replicon TaskListService1.svc/GetData request payload
    """
    return {
        'page': '1',
        'pagesize': '100000',
        'columnUris': [
            'urn:replicon:task-list-column:task',
            'urn:replicon:task-list-column:full-path',
            'urn:replicon:task-list-column:enabled'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:task-list-filter:project'
            },
            'operatorUri': 'urn:replicon:filter-operator:equal',
            'rightExpression': {
                'value': {
                    'uri': project_uri
                }
            }
        }
    }


def build_replicon_search_entry_by_oef_payload(worklog_id: str, oef_column_uri: str, oef_filter_uri: str) -> Dict[str, Any]:
    """
    Build payload to search for an existing time entry by Worklog_ID OEF value.

    Uses TimeEntryRevisionGroupListService1.svc/GetData with a filter on the
    dynamically-discovered OEF column and filter URIs.

    Args:
        worklog_id: JIRA worklog ID to search for
        oef_column_uri: Dynamically discovered OEF column URI
        oef_filter_uri: Dynamically discovered OEF filter definition URI

    Returns:
        Replicon TimeEntryRevisionGroupListService1 GetData request payload
    """
    return {
        'page': '1',
        'pagesize': '10',
        'columnUris': [
            'urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group',
            'urn:replicon:time-entry-revision-group-list-column:entry-date',
            'urn:replicon:time-entry-revision-group-list-column:approval-status',
            'urn:replicon:time-entry-revision-group-list-column:hours',
            oef_column_uri
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': oef_filter_uri
            },
            'operatorUri': 'urn:replicon:filter-operator:equal',
            'rightExpression': {
                'value': {
                    'text': str(worklog_id)
                }
            }
        }
    }


def build_replicon_delete_entry_payload(entry_uri: str) -> Dict[str, Any]:
    """
    Build Replicon API payload to delete a time entry revision group.

    Used when a worklog_updated event changes the date - the existing entry
    must be deleted and a new one created on the new date.

    Args:
        entry_uri: URI of the time entry revision group to delete

    Returns:
        Replicon DeleteTimeEntryRevisionGroup request payload
    """
    return {
        'timeEntryRevisionGroupUri': entry_uri
    }


def build_replicon_time_entry_payload(jira_data: Dict[str, Any], user: Dict[str, Any], project: Dict[str, Any], task: Optional[Dict[str, Any]], oef_definition_uri: Optional[str] = None, existing_entry_uri: Optional[str] = None, activity_uri: Optional[str] = None, jira_id_oef_definition_uri: Optional[str] = None, default_billable: bool = False) -> Dict[str, Any]:
    """
    Build Replicon PutTimeEntryRevisionGroup payload for create or update.

    For create: target.uri is None (new entry).
    For update: target.uri is the existing entry URI found via OEF search.

    Args:
        jira_data: Extracted JIRA worklog data
        user: Replicon user data
        project: Replicon project data
        task: Replicon task data (required - time posted against leaf task)
        config: Configuration object
        oef_definition_uri: OEF definition URI for Worklog_ID (hidden field,
                            numeric worklog ID — used for idempotency search)
        existing_entry_uri: If set, this is an update - the URI of the existing
                            time entry revision group to overwrite
        activity_uri: Resolved Activity URI (via lookup_activity), if
                     config.rep_hardcoded_activity_name is configured
        jira_id_oef_definition_uri: OEF definition URI for JIRA_ID (visible
                                    field — stores the issue key, e.g. TTI-148)
        default_billable: Fallback billability when the project has no
                         timeAndExpenseEntryType set (maps to config.rep_default_billable)

    Returns:
        Replicon PutTimeEntryRevisionGroup request payload
    """
    entry_date = jira_date_to_replicon(jira_data["started"])
    interval = seconds_to_replicon_interval(jira_data["time_spent_seconds"])

    comments = jira_data.get("comment") or build_replicon_comments(jira_data["issue_key"], jira_data["summary"])

    worklog_id = str(jira_data["worklog_id"])

    if task and task.get("uri"):
        main_metadata = {
            'keyUri': 'urn:replicon:time-entry-metadata-key:task',
            'value': {'uri': task["uri"]}
        }
    else:
        main_metadata = {
            'keyUri': 'urn:replicon:time-entry-metadata-key:project',
            'value': {'uri': project["uri"]}
        }

    billable_type_uri = project.get('billable_type_uri') if project else None
    if billable_type_uri is not None:
        is_billable = billable_type_uri == 'urn:replicon:time-and-expense-entry-type:billable'
    else:
        is_billable = default_billable

    custom_metadata = [
        main_metadata,
        {
            'keyUri': 'urn:replicon:time-entry-metadata-key:comments',
            'value': {'text': comments}
        },
        {
            'keyUri': 'urn:replicon:time-entry-metadata-key:is-billable',
            'value': {'bool': is_billable}
        }
    ]

    if activity_uri:
        custom_metadata.append({
            'keyUri': 'urn:replicon:time-entry-metadata-key:activity',
            'value': {'uri': activity_uri}
        })

    extension_field_values = []

    if oef_definition_uri:
        extension_field_values.append({
            'definition': {'uri': oef_definition_uri, 'name': None},
            'textValue': worklog_id
        })

    if jira_id_oef_definition_uri:
        extension_field_values.append({
            'definition': {'uri': jira_id_oef_definition_uri, 'name': None},
            'textValue': str(jira_data.get('issue_key', ''))
        })

    return {
        'timeEntryRevisionGroup': {
            'target': {'uri': existing_entry_uri, 'parameterCorrelationId': worklog_id},
            'user': {'uri': user["uri"]},
            'entryDate': entry_date,
            'timeAllocationTypeUris': [
                'urn:replicon:time-allocation-type:attendance',
                'urn:replicon:time-allocation-type:project'
            ],
            'interval': {'hours': interval, 'timePair': None},
            'customMetadata': custom_metadata,
            'extensionFieldValues': extension_field_values
        },
        'unitOfWorkId': str(uuid4())
    }
