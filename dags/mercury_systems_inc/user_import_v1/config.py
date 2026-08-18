region = 'us-east-1'
environment = 'pre-production'

time_zone = "America/New_York"

master_dag_interval = 60
execution_timeout_days = 14
gather_user_logs_timeout_hours = 2
gather_errors_from_child_timeout_hours = 2

file_sensor_timeout = 10

thread_pool_size_write_csv = 10
execution_timeout_mins_write_csv = 90

trigger_parallel_dagrun_count_process_disabled_users = 5
trigger_parallel_dagrun_count_process_active_users = 5  # CHANGE THIS in UAT

max_active_runs_master = 1
max_active_runs_process_groups = 1
max_active_runs_process_log_generation = 1
max_active_runs_process_each_user_payload = 5
max_active_runs_process_disable_rehire_users = 5
max_active_runs_new_update_users = 5
max_active_runs_process_supervisor = 5
max_active_runs_stop_accrual_for_timeoff = 2
max_active_runs_child = 5

#This is defined in custom_methods.py and request_payload.py as well, if any change is required, please update there as well
DATE_FORMAT = "%Y-%m-%d"

ENABLE_STATUS = ('A', 'P')
DISABLE_STATUS = ('L','S','T','R','D')
