region = 'eu-central-1'
environment = 'pre-production'

location = 'United kingdom'

execution_timeout_days = 14
max_active_runs = 1
payroll_export_max_active_runs = 3
export_oncall_max_active_runs = 3
export_overtime_max_active_runs = 3
write_csv_thread_pool_size = 10

time_zone = "Etc/UTC"
schedule_interval = "0 8 2 * *"
execution_timeout_mins_write_csv = 90

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

overtime_export_headers = ["Employee Number", "Business Unit", "Employee Name", "Initials", "Rate",
                           "Hours", "Incurred Period", "Entry Date", "Payment"]
oncall_export_headers = ["Employee Number", "Date",
                         "Business Unit", "Employee Name", "Initials", "Payment"]
payroll_export_file_format = "United Kingdom Payroll Export"
