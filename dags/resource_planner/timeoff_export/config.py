# Shared configuration for resource_planner_timeoff_export DAG

# DAG configuration defaults
max_active_runs = 1
schedule_interval = None

region = 'us-east-1'
environment = 'pre-production'

# Target table for timeoff data
target_table = 'dbo.dummy_rp_source'

# TimeDataExportService1.svc script filter
export_script_display_text = 'Time Off Export'

# Report-based export
report_name = 'Resource Planner TimeOff Booking Export'
deleted_report_name = 'Resource Planner Deleted TimeOff Booking Export'

# -----------------------------------------------------------------------------
# Failure-notification email (override per-instance)
# -----------------------------------------------------------------------------
# The DAG sends one email per run when any critical task failed. No DB write.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP TimeOff Bookings Export]"
