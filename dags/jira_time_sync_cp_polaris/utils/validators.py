"""
JIRA Time Sync Integration - Validators
========================================

This module contains validation functions for JIRA webhooks and
Replicon Polaris data.

NOTE: Costpoint validators are not part of the current release —
see ../backup/costpoint_integration_backup.py.
"""

from typing import Dict, Any, Optional, Tuple


def validate_jira_webhook(payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate incoming JIRA system webhook worklog payload.

    JIRA system webhooks post:
    {
        "webhook": {
            "data": {
                "webhookEvent": "worklog_created",
                "worklog": {
                    "id": "10003",
                    "issueId": "10000",
                    "timeSpentSeconds": 28800,
                    "started": "2026-07-26T04:25:07.197+0530",
                    "author": {"accountId": "619f96b4b0b630006ad2f88e", ...},
                    "comment": "..."
                }
            }
        }
    }

    Args:
        payload: The webhook payload from JIRA system webhook

    Returns:
        Tuple of (is_valid, error_message)
    """
    data = payload.get("webhook", {}).get("data", {})

    if not data:
        data = payload

    worklog = data.get("worklog", {})

    if not worklog:
        return (False, 'Missing worklog object in payload')

    if not worklog.get("id"):
        return (False, 'Missing worklog.id field')

    event_type = data.get("webhookEvent")

    if not event_type:
        return (False, 'Missing webhookEvent field')

    allowed_events = ['worklog_created', 'worklog_updated', 'worklog_deleted']

    if event_type not in allowed_events:
        return (False, f"Unsupported webhookEvent: {event_type}")

    if not worklog.get("issueId"):
        return (False, 'Missing worklog.issueId field')

    if event_type != 'worklog_deleted':
        try:
            time_spent = float(worklog.get("timeSpentSeconds", 0))
        except (TypeError, ValueError):
            return (False, f"Invalid timeSpentSeconds: {worklog.get('timeSpentSeconds')}")

        if time_spent <= 0:
            return (False, f"Invalid timeSpentSeconds: {time_spent}")

        if not worklog.get("started"):
            return (False, 'Missing worklog.started field')

    author = worklog.get("author") or {}

    if not author.get("accountId"):
        return (False, 'Missing worklog.author.accountId field')

    return (True, None)
