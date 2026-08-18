region = 'us-east-1'
environment = 'pre-production'

aws_conn_id = 'replicon.workato_S3_account'
s3_bucket_name = 'replicon-integrations-useast'

max_active_runs_master = 1
master_dag_interval = 30
file_sensor_timeout = 10
execution_timeout_days = 14
gather_user_logs_timeout_hours = 2

max_active_runs_process_groups = 1
max_active_runs_process_locations = 1
max_active_runs_process_departments = 1
max_active_runs_process_employee_types = 1
max_active_runs_process_supervisor = 5
max_active_runs_process_users = 10
max_active_runs_process_new_users = 10
max_active_runs_process_update_users = 10
max_active_runs_process_log_generation = 1

trigger_parallel_dagrun_count_process_users = 10

PASSWORD_ENCRYPTION_VARIABLE = 'lds_user_import_password_encryption_key'
