region = 'eu-central-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14
execution_timeout_days_for_posting = 1

time_zone = "Etc/UTC"
daily_run_schedule_interval = "0 * * * *"
monthly_run_schedule_interval = "0 23 1 * *"

time_export_file_format = "***Time Export"
sumo_conn_id = 'sumologic-exportlogger'
user_process_max_active_run = 5
timeentry_process_max_active_run = 15
parallel_trigger_run_count=5

post_to_endpoint_max_active_run = 3
API_JSON_PAYLOAD_LIMIT = 250
