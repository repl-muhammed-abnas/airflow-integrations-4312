region = 'us-east-1'
environment = 'pre-production'

schedule_interval='00 16 * * 1-5'
timezone= "Asia/Kolkata"

max_active_runs = 1
duration_days = 28
execution_timeout_days = 15
child_dag_max_active_runs = 10

report_name = "**TimeEntryDetails_ForOverDueNotifications"
