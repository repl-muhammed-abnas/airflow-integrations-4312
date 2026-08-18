region = "us-east-2"
environment = "pre-production"

timezone = "UTC"
max_active_runs_master = 1
max_active_runs_each_month_child = 1
max_active_runs_submit_timesheet_child = 3
execution_timeout_days = 14
gather_user_logs_timeout_hours = 1
 
report_name = "Timesheet Autosubmission - LOA"
batch_size = 50

schedule_interval = "0 1 * * 6"  # Every Saturday at 1 AM UTC
