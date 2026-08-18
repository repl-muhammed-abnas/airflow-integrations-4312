region = 'eu-central-1'
environment = 'pre-production'

time_zone = "Europe/Warsaw"

master_dag_interval = 60
execution_timeout_days = 14
gather_user_logs_timeout_hours = 2
gather_response_from_dag_runs_timeout_hours = 10

responses_from_child_timeout = 10

process_each_user_trigger_parallel_count = 10
child_add_foreign_supervisor_trigger_parallel_count = 5
child_add_supervisor_trigger_parallel_count = 5

max_active_runs_master = 1
max_active_runs_child = 2
max_active_runs_sub_child = 1
max_active_runs_process_each_user = 2

thread_pool_size_write_csv = 10
execution_timeout_mins_write_csv = 90

file_sensor_timeout = 10

DATE_DEFAULT_FORMAT = "%d/%m/%Y"
