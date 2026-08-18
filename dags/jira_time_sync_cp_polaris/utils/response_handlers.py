"""
JIRA Time Sync Integration - Response Handlers
===============================================

This module contains functions to parse and extract data
from Replicon Polaris API responses.

NOTE: Costpoint response handlers are not part of the current release —
see ../backup/costpoint_integration_backup.py.
"""

from typing import Dict, Any, Optional, List


def extract_replicon_user(response: Dict[str, Any], expected_email: str) -> Optional[Dict[str, Any]]:
    """
    Extract user data from a UserListService1.svc/GetData email-address
    equality-filter response. The payload uses
    urn:replicon:user-list-filter:email-address with
    urn:replicon:filter-operator:equal, which is an exact server-side match,
    so normally at most one row is returned.

    The response is still cross-checked row-by-row against expected_email
    (case-insensitive) as a defensive layer — zero or more than one exact
    match is treated as "not found" to avoid ever silently syncing to the
    wrong person.

    Args:
        response: Replicon API response — {'header': [...], 'rows': [{'cells': [...]}]}
            with columns in order: user, enabled, login-name, email-address
            (see build_replicon_user_search_payload)
        expected_email: The JIRA worklog author's email to match exactly

    Returns:
        Dictionary with user data, or None if not found or ambiguous
    """
    if not response or not expected_email:
        return None

    try:
        rows = response.get("rows") or []
        expected_lower = expected_email.strip().lower()
        matches = []

        for row in rows:
            cells = row.get("cells") or []

            if len(cells) < 4:
                continue

            email_cell = cells[3]
            actual_email = (email_cell.get("textValue") or "").strip().lower()

            if actual_email == expected_lower:
                matches.append(cells)

        if len(matches) != 1:
            return None

        user_cell, enabled_cell, login_name_cell, email_cell = matches[0]

        return {
            'uri': user_cell.get("uri"),
            'display_name': user_cell.get("textValue"),
            'is_enabled': enabled_cell.get("boolValue", False),
            'login_name': login_name_cell.get("textValue"),
            'email': email_cell.get("textValue")
        }
    except (KeyError, IndexError, TypeError):
        return None


def extract_replicon_activity(response: Dict[str, Any], expected_name: str) -> Optional[Dict[str, Any]]:
    """
    Extract activity data from an ActivityListService1.svc/GetData text-search
    response, filtering for an exact (case-insensitive) match on the activity
    name — same defensive pattern as extract_replicon_user, since
    text-search is fuzzy and can return more than one row.

    Args:
        response: Replicon API response — {'header': [...], 'rows': [{'cells': [...]}]}
            with columns in order: activity, code (see
            build_replicon_activity_search_payload)
        expected_name: The activity name to match exactly, e.g. "Work From Home"

    Returns:
        Dictionary with uri and name, or None if not found or ambiguous
    """
    if not response or not expected_name:
        return None

    try:
        rows = response.get("rows") or []
        expected_lower = expected_name.strip().lower()
        matches = []

        for row in rows:
            cells = row.get("cells") or []

            if not cells:
                continue

            activity_cell = cells[0]
            actual_name = (activity_cell.get("textValue") or "").strip().lower()

            if actual_name == expected_lower:
                matches.append(activity_cell)

        if len(matches) != 1:
            return None

        return {
            'uri': matches[0].get("uri"),
            'name': matches[0].get("textValue")
        }
    except (KeyError, IndexError, TypeError):
        return None


def extract_replicon_timesheet(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract timesheet data from Replicon GetTimesheetDetailsForDate response.

    Args:
        response: Replicon API response

    Returns:
        Dictionary with timesheet data or None if not found
    """
    if not response:
        return None

    try:
        timesheet = response.get("timesheet", {})

        if not timesheet:
            return None

        status_uri = timesheet.get("statusUri", "")
        status = status_uri.split(":")[-1] if status_uri else "unknown"

        return {
            'uri': timesheet.get("uri"),
            'status_uri': status_uri,
            'status': status,
            'date_range': timesheet.get("dateRange"),
            'user_uri': timesheet.get("user", {}).get("uri")
        }
    except (KeyError, TypeError):
        return None


def extract_replicon_project(response: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Extract project data from Replicon BulkGetProjectDetails3 response.

    Args:
        response: Replicon API response (list of project objects)

    Returns:
        Dictionary with project data or None if not found
    """
    if not response:
        return None

    try:
        project_record = response[0]
        project_details = project_record.get("projectDetails", {})

        if not project_details:
            return None

        status_uri = project_details.get("status", {}).get("uri", "")
        is_active = "active" in status_uri.lower()

        time_expense_type = project_details.get("timeAndExpenseEntryType") or {}

        return {
            'uri': project_details.get("uri"),
            'name': project_details.get("name"),
            'code': project_details.get("code"),
            'description': project_details.get("description"),
            'client_uri': project_details.get("client", {}).get("uri"),
            'client_name': project_details.get("client", {}).get("name"),
            'status_uri': status_uri,
            'is_active': is_active,
            'date_range': project_details.get("dateRange"),
            'allows_time_entry': project_details.get("allowTimeEntryAgainstProject", True),
            'is_closed': project_details.get("isClosed", False),
            'billable_type_uri': time_expense_type.get("uri")
        }
    except (KeyError, IndexError, TypeError):
        return None


def extract_task_by_full_path(response: Dict[str, Any], task_path: List[str]) -> Optional[Dict[str, Any]]:
    """
    Find a task from TaskListService1.svc/GetData response by matching its
    full hierarchical path against the expected task_path segments.

    The full-path column (cell index 1) returns a cellCollection list where
    each element's textValue is one path segment from root to leaf task.
    Matching [cellCollection[0].textValue, ...cellCollection[-1].textValue]
    against task_path handles any task depth without level-by-level traversal.

    Args:
        response: Parsed API response dict - {'d': {'rows': [...]}}
        task_path: Ordered list of task name segments from root to leaf,
                   e.g. ['DICE - Integrations', 'USA User Integration v4.4', 'Development']

    Returns:
        Dict {'uri', 'name', 'is_closed'} for the matching task, or None.
    """
    if not response or not task_path:
        return None

    null_urn = "urn:replicon:list-type:null"

    try:
        rows = response.get("rows") or []

        for row in rows:
            cells = row.get("cells") or []

            if len(cells) < 3:
                continue

            enabled_cell = cells[2]

            if enabled_cell.get("dataType") == null_urn:
                continue

            if enabled_cell.get("textValue") != "True":
                continue

            cell_collection = cells[1].get("cellCollection") or []
            path_segments = [x.get("textValue", "") for x in cell_collection]

            if path_segments != task_path:
                continue

            task_cell = cells[0]

            if task_cell.get("dataType") == null_urn:
                continue

            return {
                'uri': task_cell.get("uri"),
                'name': task_cell.get("textValue"),
                'is_closed': False
            }
    except (KeyError, TypeError, IndexError):
        return None

    return None


def extract_existing_time_entry(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract time entry URI from TimeEntryRevisionGroupListService GetData response.

    Used when searching for an existing time entry by Worklog_ID OEF value
    to support worklog_updated handling (update existing entry).

    Expected columns (from search payload):
        Cell 0: time-entry-revision-group (entry URI)
        Cell 1: entry-date
        Cell 2: approval-status
        Cell 3: hours
        Cell 4: OEF column (Worklog_ID text value)

    Args:
        response: Replicon TimeEntryRevisionGroupListService1 GetData response

    Returns:
        Dictionary with entry_uri, entry_date, entry_hours, and oef_value,
        or None if no valid match
    """
    null_urn = "urn:replicon:list-type:null"

    if not response:
        return None

    try:
        rows = response.get("rows") or []

        if not rows:
            return None

        cells = rows[0].get("cells") or []

        if len(cells) < 5:
            return None

        entry_cell = cells[0]
        entry_uri = entry_cell.get("uri")

        if not entry_uri or entry_cell.get("dataType") == null_urn:
            return None

        date_cell = cells[1]
        entry_date = date_cell.get("textValue") if date_cell.get("dataType") != null_urn else None

        hours_cell = cells[3]
        if hours_cell.get("dataType") == null_urn:
            entry_hours = None
        elif hours_cell.get("textValue") is not None:
            entry_hours = hours_cell["textValue"]
        elif hours_cell.get("numericValue") is not None:
            entry_hours = str(round(float(hours_cell["numericValue"]), 2))
        elif hours_cell.get("calendarDayDurationValue"):
            dur = hours_cell["calendarDayDurationValue"]
            total = (dur.get("hours") or 0) + (dur.get("minutes") or 0) / 60 + (dur.get("seconds") or 0) / 3600
            entry_hours = str(round(total, 2))
        else:
            entry_hours = None

        oef_cell = cells[4]
        oef_value = oef_cell.get("textValue")

        if not oef_value or oef_cell.get("dataType") == null_urn:
            return None

        return {
            'entry_uri': entry_uri,
            'entry_date': entry_date,
            'entry_hours': entry_hours,
            'oef_value': oef_value
        }
    except (KeyError, IndexError, TypeError):
        return None
