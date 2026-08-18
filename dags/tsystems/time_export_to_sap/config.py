region = 'eu-central-1'
environment = "pre-production"

pacific_timezone = 'Etc/UTC'

dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

dag_max_active_tasks = 128
master_dag_max_active_runs = 1
child_max_active_runs = 5

execution_timeout_days = 14

sumo_conn_id = 'sumologic-exportlogger'

default_file_format = "TimeExport_SAP"

schedule_interval = "0 23 * * *"  # Run daily at 11:00 PM UTC every day
