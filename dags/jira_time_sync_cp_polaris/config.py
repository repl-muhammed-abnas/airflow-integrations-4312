"""
JIRA Time Sync Integration - Base Configuration
================================================

This module contains the base configuration for the JIRA to
Replicon Polaris time sync integration.

NOTE: Costpoint sync is not part of the current release. Its config,
DAG, and helper functions are backed up in full (self-contained) at
backup/costpoint_integration_backup.py.

Instance-specific configurations should import from this file
and override values as needed.
"""
region = "us-east-1"
environment = "pre-production"

schedule_interval = None
max_active_runs = 10
execution_timeout_days = 1

replicon_child_max_active_runs = 1

jira_project_custom_field = "customfield_10062"

jira_task_custom_field = None

JIRA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
ENTRY_DATE_FORMAT = "%Y-%m-%d"

rep_default_billable = False

rep_require_enabled_user = True

# Hidden OEF — stores the numeric JIRA worklog ID (e.g. "44294").
# Used for idempotency: search/filter on this field to find an existing
# entry before deciding create vs update.
REP_OEF_WORKLOG_ID_NAME = "Worklog_ID"

# Visible OEF — stores the JIRA issue key (e.g. "TTI-148").
# Shown to users in the Replicon time entry editor as "JIRA_ID".
REP_OEF_JIRA_ID_NAME = "JIRA_ID"
