region = "eu-central-1"
environment = "pre-production"

master_dag_interval = 30
file_sensor_timeout = 10
execution_timeout_days = 14
gather_user_logs_timeout_hours = 2

max_active_runs_master = 1
max_active_runs_process_users = 10
max_active_runs_process_new_users = 10
max_active_runs_process_update_users = 10
max_active_runs_process_supervisor = 1
max_active_runs_process_time_off_type_assignment_new_user = 10
max_active_runs_process_log_generation = 1

max_active_runs_process_groups = 1
max_active_runs_process_countries = 1
max_active_runs_process_departments = 1
max_active_runs_process_employee_type = 1
max_active_runs_process_full_part_time = 1
max_active_runs_process_standard_hours = 1
max_active_runs_process_work_locations = 1

trigger_parallel_dagrun_count_process_users = 10

TIMESHEET_PERIOD = 'Monthly'

REFERENCE_TIME_OFF_AUSTRIA = 'Austria Compensation Day'
REFERENCE_TIME_OFF_GERMANY = 'Germany Compensation Day'
