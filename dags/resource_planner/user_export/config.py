# Shared configuration for resource_planner_user_export DAG

region = 'us-east-1'
environment = 'pre-production'

# DAG configuration defaults
max_active_runs = 1
schedule_interval = None

# Report configuration
report_name = "Resource Planner Users Report"

# Lookup tables
resources_table = 'dbo.rp_resources'

# -----------------------------------------------------------------------------
# Failure-notification email (override per-instance)
# -----------------------------------------------------------------------------
# The DAG sends one email per run when any critical task failed. No DB write.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP User Export]"
