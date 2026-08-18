region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14

time_zone = "US/Eastern"
schedule_interval = "30 14 * * SUN"

time_export_file_format = "CRL Replicon to SAP ECC"
sumo_conn_id = 'sumologic-exportlogger'
