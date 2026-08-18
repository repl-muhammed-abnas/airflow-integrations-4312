region = 'eu-central-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14

time_zone = "Etc/UTC"
daily_run_schedule_interval = "30 1,3,5,7,9,11,13,15,17,19,21,23 2-28,29,30,31 * *"
monthly_run_schedule_interval = "30 12 1 * *"

payroll_export_file_format = "BEL Payroll Export"
sumo_conn_id = 'sumologic-exportlogger'

API_JSON_PAYLOAD_LIMIT = 250
