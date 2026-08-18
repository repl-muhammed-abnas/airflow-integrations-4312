# Shared configuration for resource_planner_project_task_export_bulk DAG

# DAG configuration defaults
max_active_runs = 1
max_active_runs_child = 20
schedule_interval = None

region = 'us-east-1'
environment = 'pre-production'

# Report configuration
project_uri_report_name = "Resource Planner Project URI Report"
project_task_report_name = "Resource Planner Project Task Report"
user_report_name = "Resource Planner Users Report"
BATCH_SIZE = 20
INSERT_BATCH_SIZE = 500

# Target table for project/task data
target_table = 'dbo.dummy_rp_source_time_codes'

# -----------------------------------------------------------------------------
# Failure-notification email (override per-instance)
# -----------------------------------------------------------------------------
# The master DAG sends one email per run when one or more critical tasks
# failed in the master or in any child batch. No DB write.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP Project Task Export Bulk]"