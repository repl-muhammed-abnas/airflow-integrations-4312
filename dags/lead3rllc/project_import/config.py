region = 'us-east-1'
environment = 'pre-production'

time_zone = 'US/Pacific'

master_dag_interval = 30
max_active_runs_master = 1
max_active_runs_child = 1
max_active_runs_process_log_generation = 1

file_sensor_timeout = 10

execution_timeout_days = 14
execution_timeout_mins_write_csv = 90

parallel_dagrun_count = 2
