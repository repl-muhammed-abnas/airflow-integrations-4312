region = 'eu-central-1'
environment = 'pre-production'

max_active_runs = 1
schedule_interval = "0 0,4,8,12,16,20 * * *"
time_zone = "UTC"

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

thread_pool_size_write_csv = 10
execution_timeout_mins_write_csv = 90

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

logfile_columns = ["Process Started","File Name","File Path","Location","Total Hours","Number of Records","Date Range"]
