"""
JIRA Time Sync Integration - Data Transformers
===============================================

This module contains functions for transforming data between
JIRA and Replicon Polaris formats.

NOTE: Costpoint-only transforms (build_costpoint_notes, jira_date_to_costpoint)
are not part of the current release — see ../backup/costpoint_integration_backup.py.
"""

from datetime import datetime
from typing import Dict, Any, Optional


def seconds_to_decimal_hours(seconds: int) -> float:
    """
    Convert seconds to decimal hours (used for display in JIRA comments).

    Args:
        seconds: Time in seconds from JIRA worklog

    Returns:
        Decimal hours rounded to 2 decimal places

    Examples:
        >>> seconds_to_decimal_hours(300)   # 5 minutes
        0.08
        >>> seconds_to_decimal_hours(3600)  # 1 hour
        1.0
        >>> seconds_to_decimal_hours(5400)  # 1.5 hours
        1.5
    """
    if not seconds or seconds <= 0:
        return 0.0

    return round(seconds / 3600, 2)


def seconds_to_replicon_interval(seconds: int) -> Dict[str, int]:
    """
    Convert seconds to Replicon interval object format.

    Uses the PutTimeEntryRevisionGroup format (hours, minutes, seconds only).

    Args:
        seconds: Time in seconds from JIRA worklog

    Returns:
        Dictionary with hours, minutes, seconds

    Examples:
        >>> seconds_to_replicon_interval(300)   # 5 minutes
        {'hours': 0, 'minutes': 5, 'seconds': 0}
        >>> seconds_to_replicon_interval(3665)  # 1 hour, 1 min, 5 sec
        {'hours': 1, 'minutes': 1, 'seconds': 5}
    """
    if not seconds or seconds <= 0:
        return {
            'hours': 0,
            'minutes': 0,
            'seconds': 0
        }

    hours = seconds // 3600
    remaining = seconds % 3600
    minutes = remaining // 60
    secs = remaining % 60

    return {
        'hours': hours,
        'minutes': minutes,
        'seconds': secs
    }


def parse_jira_datetime(jira_date_str: str) -> datetime:
    """
    Parse JIRA datetime string to Python datetime object.

    JIRA format: "2026-01-28T17:16:38.000+0000"

    Args:
        jira_date_str: Date string from JIRA

    Returns:
        Python datetime object
    """
    clean_date = jira_date_str[:19]

    return datetime.strptime(clean_date, "%Y-%m-%dT%H:%M:%S")


def jira_date_to_replicon(jira_date_str: str) -> Dict[str, int]:
    """
    Convert JIRA date string to Replicon date object format.

    Args:
        jira_date_str: Date string from JIRA (e.g., "2026-01-28T17:16:38.000+0000")

    Returns:
        Dictionary with year, month, day

    Examples:
        >>> jira_date_to_replicon("2026-01-28T17:16:38.000+0000")
        {'year': 2026, 'month': 1, 'day': 28}
    """
    dt = parse_jira_datetime(jira_date_str)

    return {
        'year': dt.year,
        'month': dt.month,
        'day': dt.day
    }


def jira_date_to_date_string(jira_date_str: str) -> str:
    """
    Extract date portion from JIRA datetime string.

    Args:
        jira_date_str: Date string from JIRA

    Returns:
        Date string in YYYY-MM-DD format
    """
    return jira_date_str[:10]


def extract_worklog_comment(comment_field: Any) -> str:
    """
    Extract plain text from a JIRA worklog comment field.

    JIRA Cloud returns comments in Atlassian Document Format (ADF):
        {"type": "doc", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "My comment"}]}]}
    JIRA Server returns a plain string.

    Args:
        comment_field: Raw comment value from JIRA worklog

    Returns:
        Plain text string, or empty string if no comment
    """
    if not comment_field:
        return ""

    if isinstance(comment_field, str):
        return comment_field

    if isinstance(comment_field, dict):
        return _extract_adf_text(comment_field).strip()

    return ""


def _extract_adf_text(node: Dict[str, Any]) -> str:
    """Recursively extract plain text from an ADF node."""
    if node.get("type") == "text":
        return node.get("text", "")

    parts = [_extract_adf_text(child) for child in node.get("content", [])]

    separator = " " if node.get("type") in ('doc', 'paragraph', 'bulletList', 'orderedList') else ""

    return separator.join(filter(None, parts))


def extract_custom_field_value(field_value: Any) -> Optional[str]:
    """
    Extract value from JIRA custom field which can have various structures.

    JIRA custom fields can be:
    - Simple string: "value"
    - Object with value: {"value": "value"}
    - Object with name: {"name": "value"}
    - Array of objects: [{"value": "value"}]
    - User picker: {"accountId": "...", "displayName": "..."}

    Args:
        field_value: The raw custom field value from JIRA

    Returns:
        Extracted string value or None
    """
    if field_value is None:
        return None

    if isinstance(field_value, str):
        return field_value

    if isinstance(field_value, dict):
        for key in ('value', 'name', 'displayName', 'key', 'id'):
            if key in field_value:
                return str(field_value[key])

        return None

    if isinstance(field_value, list) and field_value:
        first_item = field_value[0]

        if isinstance(first_item, str):
            return first_item

        if isinstance(first_item, dict):
            for key in ('value', 'name', 'displayName', 'key', 'id'):
                if key in first_item:
                    return str(first_item[key])

            return None

    return None


def extract_jira_worklog_data(webhook_payload: Dict[str, Any], issue_response: Dict[str, Any], config, author_response: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Extract relevant data from a JIRA system webhook worklog payload,
    combined with the fetched issue and author details.

    Args:
        webhook_payload: The full webhook payload from JIRA system webhook:
            {"webhook": {"data": {"webhookEvent": "worklog_created",
                "worklog": {id, issueId, timeSpentSeconds, started, author, comment}}}}
        issue_response: JIRA GET /issue/{issueId} response — resolves issueKey,
            summary, and project/task custom fields. For non-delete events the
            author email is also extracted from the embedded worklogs list by id.
        config: Configuration object with field mappings
        author_response: JIRA GET /user?accountId=... response — used as fallback
            for email when the worklog is no longer in the issue's embedded list
            (always the case for worklog_deleted events).

    Returns:
        Dictionary with extracted worklog data
    """
    data = webhook_payload.get("webhook", {}).get("data", {})

    if not data:
        data = webhook_payload

    worklog = data.get("worklog", {})
    worklog_author = worklog.get("author") or {}
    account_id = worklog_author.get("accountId", "")

    issue = issue_response
    if isinstance(issue, list):
        issue = issue[0] if issue else {}
    issue = issue or {}
    issue_fields = issue.get("fields", {})

    # Try to resolve author email from the issue's embedded worklog list first.
    # Falls back to author_response (GET /user?accountId=...) which is always
    # populated — covers delete events where the worklog is already gone.
    worklog_id = worklog.get("id", "")
    email = ""
    for wl in (issue_fields.get("worklog") or {}).get("worklogs", []):
        if str(wl.get("id")) == str(worklog_id):
            email = (wl.get("author") or {}).get("emailAddress", "")
            break

    if not email and author_response:
        author_data = author_response
        if isinstance(author_data, list):
            author_data = author_data[0] if author_data else {}
        email = (author_data or {}).get("emailAddress", "")

    issue_key = issue.get("key", "")
    summary = issue_fields.get("summary", "")

    raw_project_code = issue_fields.get(config.jira_project_custom_field)
    project_code = extract_custom_field_value(raw_project_code) or ""

    task_name = None
    if config.jira_task_custom_field:
        raw_task_name = issue_fields.get(config.jira_task_custom_field)
        task_name = extract_custom_field_value(raw_task_name)

    time_spent_seconds = int(float(worklog.get("timeSpentSeconds", 0)))
    started = worklog.get("started", "")
    worklog_comment = extract_worklog_comment(worklog.get("comment", ""))

    notes = f"{issue_key}"
    if summary:
        notes = f"{issue_key} - {summary}"

    webhook_event = data.get("webhookEvent", "unknown")

    return {
        'worklog_id': worklog_id,
        'issue_key': issue_key,
        'issue_id': worklog.get("issueId"),
        'account_id': account_id,
        'email': email,
        'project_code': project_code,
        'task_name': task_name,
        'time_spent_seconds': time_spent_seconds,
        'hours_decimal': seconds_to_decimal_hours(time_spent_seconds),
        'started': started,
        'date_string': jira_date_to_date_string(started) if started else "",
        'summary': summary,
        'comment': worklog_comment,
        'notes': notes[:254] if notes else "",
        'webhook_event': webhook_event
    }


def parse_jira_path_field(path_string: str) -> Dict[str, Any]:
    """
    Parse a colon-separated Replicon path from a JIRA custom field.

    Input:  "02479284- Assign [USA]... : DICE - Integrations : USA User Integration v4.4 : Development"
    Output: {
        'project_name': '02479284- Assign [USA]...',
        'task_path':    ['DICE - Integrations', 'USA User Integration v4.4', 'Development']
    }

    Splits on ' : ' (space-colon-space) to preserve hyphens within names.
    """
    if not path_string or not isinstance(path_string, str):
        return {
            'project_name': None,
            'task_path': []
        }

    segments = [s.strip() for s in path_string.split(" : ")]

    return {
        'project_name': segments[0] if segments else None,
        'task_path': segments[1:] if len(segments) > 1 else []
    }


def build_replicon_comments(issue_key: str, summary: str, max_length: int = 500) -> str:
    """
    Build comments for Replicon time entry metadata.

    Args:
        issue_key: JIRA issue key (e.g., "KAN-1")
        summary: JIRA issue summary
        max_length: Maximum length for comments

    Returns:
        Formatted comments string
    """
    comments = f"JIRA: {issue_key}"

    if summary:
        comments = f"JIRA: {issue_key} - {summary}"

    if len(comments) > max_length:
        comments = comments[:max_length - 3] + "..."

    return comments
