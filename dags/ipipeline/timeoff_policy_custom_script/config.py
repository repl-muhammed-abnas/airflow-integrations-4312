# Environment Configuration
region = 'us-east-1'
environment = 'pre-production'

# Scheduling Configuration 
master_dag_interval = 6  # hours
time_zone = 'America/New_York'
tempo_time_zone = 'Etc/UTC'

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

expected_report_columns = "User Name,User Uri,Login Name,User Start Date,Time Off Type,TimeOffTypeUri,FTE,Scheduled Hours"

schedule_interval_annual = "59 23 15 12 *"
schedule_interval_daily = "0 3 * * *"

annual_timeoff_report = "**YEAR END POLICY LINE - CUSTOM SCRIPT"
daily_timeoff_report = "**Daily POLICY LINE - CUSTOM SCRIPT"

execution_timeout_days = 14
execution_timeout_hours = 4
max_active_runs_master = 1
max_active_runs_child = 5
max_active_runs_process_log_generation = 1
process_users_dagruns_count = 10
