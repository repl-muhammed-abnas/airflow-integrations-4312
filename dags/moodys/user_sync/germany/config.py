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

# Germany defaults - spec rows R37-R47
TIMESHEET_TEMPLATE = 'Germany - In/Out Timesheet'   # R37
TIMESHEET_APPROVAL_PATH = 'Supervisor'              # R38
TIMESHEET_PERIOD = 'Monthly'                        # R39
TIMEOFF_TEMPLATE = 'Time Off'                       # R40
TIMEOFF_APPROVAL_PATH = 'Auto/system approval'      # R41
DEFAULT_WORK_WEEK = 'urn:replicon:day-of-week:monday'   # R43
EXCEPTION_WORK_WEEK = 'urn:replicon:day-of-week:sunday'
DEFAULT_SCHEDULE = 'Germany - 8hours/pay Mon - Fri'