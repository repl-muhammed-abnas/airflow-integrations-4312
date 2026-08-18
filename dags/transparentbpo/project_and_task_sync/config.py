region = 'us-east-1'
environment = 'pre-production'

time_zone = 'Asia/Kolkata'

run_date_format = "%Y-%m-%d"
log_file_timestamp_format = "%Y%m%dT%H"

execution_timeout_days = 14

final_log_generation_dag_schedule_interval = "0 0 * * *"  # Daily at midnight

max_active_runs_master = 5
max_active_runs_process_logs_pregeneration = 5
max_active_runs_send_logs = 5

lookup_log_timestamp_hours = 24