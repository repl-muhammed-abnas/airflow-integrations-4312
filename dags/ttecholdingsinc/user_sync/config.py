region = "us-east-1"
environment = "pre-production"

master_dag_interval = 30
file_sensor_timeout = 10
execution_timeout_days = 14
gather_user_logs_timeout_hours = 2


max_active_runs_master = 1
max_active_runs_process_groups = 1
max_active_runs_process_departments = 1
max_active_runs_process_users = 5
max_active_runs_process_new_users = 5
max_active_runs_process_update_users = 5
max_active_runs_process_supervisor = 5

max_active_runs_process_time_off_type_assignment_new_user = 5
max_active_runs_process_log_generation = 1
trigger_parallel_dagrun_count_process_users = 5

DATE_FORMAT = "%m/%d/%Y"

PUNCH_POLICY = "All Devices Access"
