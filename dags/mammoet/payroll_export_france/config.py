region = 'eu-central-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14

time_zone = "CET"
daily_run_schedule_interval = "0 15 1,3-28,29,30,31 * *"
monthly_run_schedule_interval = "30 15 2 * *"

payroll_export_file_format = "France Payroll Export"
sumo_conn_id = 'sumologic-exportlogger'

API_JSON_PAYLOAD_LIMIT = 250
