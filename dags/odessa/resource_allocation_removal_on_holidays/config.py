region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'Odessaafmig'
schedule_interval = "0 9 * * *"
time_zone = "America/New_York"
user_data_report_name = 'UserData_forallocation'
execution_timeout_days = 14
max_active_runs_child = 10
max_active_runs_master = 1
max_active_runs_process_child = 5
gather_user_logs_timeout_hours = 14

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_useast_s3_bucket'
