"""Base configuration for CRL Office Schedule Sync"""

region = 'us-east-1'  # AWS region
environment = 'pre-production'

# DAG Execution Settings
max_active_runs_master = 1  # Only one master run at a time
max_active_runs_child = 5  # Up to 5 parallel child DAG runs
process_log_max_active_runs = 1

parallel_count_process_schedules = 5

execution_timeout_days = 14

# Timeouts
gather_logs_timeout_hours = 12

# Timezone
time_zone = 'America/Los_Angeles'

start_date_format = "%m/%d/%Y"
