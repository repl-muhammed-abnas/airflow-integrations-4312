region = 'eu-central-1'
environment = 'pre-production'

europe_timezone = 'Europe/Paris'

max_active_runs_master = 1
child_dag_max_active_runs = 1

report_name = "Distance Traveled Report - NLD (Automation)"

# Schedule: Runs on January 15 and February 15 at 23:59 (Europe/Paris timezone)
# Processes previous calendar year data (Jan 1 - Dec 31)
schedule_interval = '59 23 15 1,2 *'

batch_size = 4

filter_name = "EntryDateFilter"

thread_pool_size_write_csv = 10
execution_timeout_write_csv = 6
execution_timeout_days = 14
