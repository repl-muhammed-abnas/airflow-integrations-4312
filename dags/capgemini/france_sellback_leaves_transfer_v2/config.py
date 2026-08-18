region = 'eu-central-1'
environment = 'pre-production'

schedule_interval = "0 1 * * *"

time_zone = "Etc/UTC"

execution_timeout_mins_write_csv = 90
execution_timeout_days = 14
gather_logs_timeout_hours = 12

expected_report_columns = "Employee ID,User Name,UserUri,Time Off Type,Units,Date,Event Type,Amount"
report_name = "France Sell Back Leaves Transfers V1"

dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'
