region = 'eu-central-1'
environment = 'pre-production'

schedule_interval = "0 1 * * *"

time_zone = "Etc/UTC"

execution_timeout_mins_write_csv = 90
execution_timeout_days = 14
thread_pool_size_write_csv = 10

dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'
