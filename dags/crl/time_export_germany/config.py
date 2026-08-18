region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14

time_zone = "Europe/Berlin"
schedule_interval = "0 15 * * 1"  # Every Monday at 3 PM CET/CEST

time_export_file_format = "CRL Replicon to SAP ECC Germany"
sumo_conn_id = 'sumologic-exportlogger'
