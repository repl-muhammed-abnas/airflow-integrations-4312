region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14

time_zone = "Europe/London"
schedule_interval = "0 13 * * *"  # Everyday at 1 PM GB Time (8 AM EST)

time_export_file_format = "CRL Replicon to SAP ECC UK"
sumo_conn_id = 'sumologic-exportlogger'