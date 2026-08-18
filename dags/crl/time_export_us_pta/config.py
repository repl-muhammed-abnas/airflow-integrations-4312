region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14

time_zone = "US/Eastern"
schedule_interval = "30 14 * * SUN,MON,WED"

division_to_ignore = '1100'

time_export_file_format = "CRL Replicon to SAP ECC US"
sumo_conn_id = 'sumologic-exportlogger'
pai_locations = ["FRDRKPAI", "DURHAMPAI", "DURHAMPAIW", "SKOKIEPAI"]
