region = 'eu-central-1'
environment = 'pre-production'
disabled = True

max_active_runs = 1
time_zone = "UTC"

dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

execution_timeout_mins_write_csv = 90

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'
