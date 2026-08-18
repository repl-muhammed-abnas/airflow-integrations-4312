region = 'us-east-1'
environment = "pre-production"

# Max active runs
process_timesheet_data_child_max_active_run = 2
replicon_qbo_time_and_timeoff_sync_child_max_active_run = 2

# Dag trigger parallel count
trigger_replicon_qbo_time_and_timeoff_sync_child_parallel_count = 2

# Schedule
clean_up_older_log_entries_schedule_interval = "0 1 * * *"

# Timeouts
execution_timeout_days = 14

# Timezone
time_zone = "America/Los_Angeles"

# Report Name
TIME_SYNC_REPORT_NAME = "Time Sync Report"

# Pay code configuration
VALID_PAY_CODES = ['Regular Pay', 'Overtime Pay', 'Vacation Pay', 'Holiday Pay', 'Sick Pay']