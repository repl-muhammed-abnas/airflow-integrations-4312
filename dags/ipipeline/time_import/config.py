"""
iPipeline JIRA-Replicon Time Import - Configuration
Integration for syncing time entries from JIRA/Tempo to Replicon
"""
from datetime import timedelta

# Environment Configuration
region = 'us-east-1'
environment = 'pre-production'

# Scheduling Configuration 
master_dag_interval = "0 */6 * * *"  #Every 6 hours
time_zone = 'America/New_York'
tempo_time_zone = 'UTC'

# trigger_parallel_count_settings
parallel_count_process_jira_issue_ids = 5
parallel_count_process_each_user_time_entries = 5

# Performance Settings - Master DAG
max_active_runs_master = 1

# Child DAG max active runs
process_jira_project_info_max_active_runs = 5
process_each_user_time_entries_max_active_runs = 5
process_each_time_entry_max_active_runs = 5
process_log_generation_max_active_runs = 1

# Timeout settings
execution_timeout_days = 14

gather_task_metadata_retrieval_logs_timeout_hours = 2
gather_logs_timeout_hours = 4
user_processing_timeout_hours = 4
entry_creation_timeout_hours = 1
log_generation_timeout_hours = 1

# API Configuration
tempo_api_base_url = "https://api.tempo.io/4"
jira_api_base_url = "https://ipipelinejira-replicon-sandbox.atlassian.net/rest/api/3"

# Date formats
ENTRY_DATE_FORMAT = "%Y-%m-%d"
REPLICON_DATE_FORMAT = "%m/%d/%Y"
ISO_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

EMAIL_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
TEMPO_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Log expiry
log_expiry = 7*24*60*60  # 7 days in seconds

MANDATORY_FIELDS = {
    'task_issue_id': 'Task Issue Id',
    'task_type': 'Task Type',
    'time_entry_comment': 'Description',
    'time_entry_date': 'Time Entry Date',
    'author_jira_account_id': 'Author JIRA Id',
    'task_parent_jira_id': 'Parent Id',
    'task_summary': 'JIRA Summary',
    'hours': 'Duration',
    'replicon_id': 'Replicon ID at Epic Level'
}

OEF_MAPPER = {
    'Parent ID': 'task_parent_jira_id',
    'JIRA Summary': 'task_summary'
}
