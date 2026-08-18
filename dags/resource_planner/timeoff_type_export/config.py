# Shared configuration for resource_planner_timeoff_type_export DAG

# DAG configuration defaults
max_active_runs = 1
schedule_interval = None

region = 'us-east-1'
environment = 'pre-production'

# -----------------------------------------------------------------------------
# Failure-notification email (override per-instance)
# -----------------------------------------------------------------------------
# The DAG sends one email per run when any critical task failed. No DB write.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP TimeOff Type Export]"
