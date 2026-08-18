region = 'eu-central-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14
execution_timeout_days_for_posting = 1

time_zone = "Etc/UTC"
daily_run_schedule_interval = "*/30 * * * *"

time_export_file_format = "SAP S4Hana H4S4"
sumo_conn_id = 'sumologic-exportlogger'

post_to_endpoint_max_active_run = 3
