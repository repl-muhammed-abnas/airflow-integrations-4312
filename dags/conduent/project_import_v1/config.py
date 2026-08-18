region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
child_max_active_runs = 10
child_parallel_count = 10
schedule_interval = "*/5 * * * *"
execution_timeout_days = 14
file_sensor_timeout = 10
process_log_generation_max_active_runs = 1
time_zone = 'EST'

project_list_report = '**Project List**'
project_manager_blob_key_name = 'project_manager_change_effective_date'
