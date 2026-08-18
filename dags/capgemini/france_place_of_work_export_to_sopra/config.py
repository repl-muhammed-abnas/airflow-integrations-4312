region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
max_active_runs = 1
max_active_process_export_runs = 6
write_csv_thread_pool_size = 10

time_zone = "UTC"

execution_timeout_mins_write_csv = 90

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

export_headers = ["paycode", "employee_id", "format", "bucket", "monsal", "last_date_of_prev_month", "entitlement"]
no_of_months_place_of_work_data_to_export = 6
