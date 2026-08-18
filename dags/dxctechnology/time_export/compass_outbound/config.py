region = 'us-east-2'
environment = 'pre-production'

utc_timezone = 'Etc/UTC'
execution_timeout_days = 14

can_run_batch_task_var_name = 'dxctechnology_compass_can_run_batch_task'

aws_conn_id = 'replicon.workato_S3_account'

record_count_limit = 140000
s3_download_link_expiry = 7*24*60*60

error_template = '{{ get_error_message() }}'
