# Shared defaults - instance-specific values are defined in instances/trial.py and instances/prod.py
# Instance files override these values using wildcard import
region = 'eu-central-1'
environment = 'pre-production'
max_active_runs = 1
max_active_runs_child = 5
max_active_delta_records_runs = 1
max_active_new_records_runs = 1
max_active_process_records_runs = 5
execution_timeout_days = 14
jobtype = 'Timeoff Export to Workday'

# Time zone settings
time_zone = 'Etc/UTC'

# Report configurations
file_format_name = 'Timeoff export sync'

# Schedule settings (default - can be overridden by instances)
schedule_interval = '0 22 * * *'  # Daily at 10 PM UTC