region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
max_active_runs = 1
write_csv_thread_pool_size = 10

time_zone = "Etc/UTC"

execution_timeout_mins_write_csv = 90

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

export_headers = ["paycode", "employee_id", "booking_start_date", "booking_end_date", "day_start_indicator",
    "day_end_indicator", "hours", "short_id", "transaction_type", "horodatage", "initialorextension", "workedstartday"]

disabled = True
