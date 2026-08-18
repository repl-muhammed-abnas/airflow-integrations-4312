region = "us-east-1"
environment = "pre-production"

# Runs every 25th of the month at 6:00 PM
schedule_interval = "0 18 25 * *"
timezone = "Asia/Kolkata"

# Timeout
execution_timeout_days = 14

# Dag configurations
max_active_run_master = 1
max_active_runs_process_each_user = 1
parallel_count_process_each_user = 2