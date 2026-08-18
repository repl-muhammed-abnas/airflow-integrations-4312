region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14

time_zone = "US/Eastern"
schedule_interval = "0 12 * * *"

division_to_ignore = ['1100']
business_unit_names = ("NA04", "NA05")
call_in_dropdowns_to_ignore = ("Shift differential 15%", "Shift differential 10%")

time_export_file_format = "CRL Replicon to SAP ECC US"
sumo_conn_id = 'sumologic-exportlogger'
pai_locations = ["FRDRKPAI", "DURHAMPAI", "DURHAMPAIW", "SKOKIEPAI"]
