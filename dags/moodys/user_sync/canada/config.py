region = "eu-central-1"
environment = "pre-production"

max_active_runs_master = 1
master_dag_interval = 30
file_sensor_timeout = 10
execution_timeout_days = 14
gather_user_logs_timeout_hours = 2

max_active_runs_process_users = 3
max_active_runs_process_new_users = 3
max_active_runs_process_update_users = 3
max_active_runs_process_supervisor = 3
max_active_runs_process_log_generation = 1
max_active_runs_process_groups = 1
max_active_runs_process_divisions = 1

trigger_parallel_dagrun_count_process_users = 10
trigger_parallel_dagrun_count_process_divisions = 1

TIMESHEET_TEMPLATE = 'Gen3 - In/Out Timesheets'
TIMESHEET_APPROVAL_PATH = 'Supervisor'
TIMESHEET_PERIOD = 'Biweekly'
TIMEOFF_TEMPLATE = 'Time Off'
TIMEOFF_APPROVAL_PATH = 'Auto/system approval'
DEFAULT_WORK_WEEK = 'urn:replicon:day-of-week:sunday'
