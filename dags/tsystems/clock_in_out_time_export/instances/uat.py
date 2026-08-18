"""UAT instance configuration for T-Systems Clock In/Out Export."""
from tsystems.clock_in_out_time_export.config import *

instance = "uat"

company_key = "tsystemsSB"
environment = "pre-production"

# SAP BTP Configuration
client_http_conn_id = f"tsystems_clock_in_out_http_connection_{instance}"
client_post_api_http_conn = f"tsystems_clock_in_out_http_post_api_connection_{instance}"

# Email notification settings
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Replicon connection
replicon_conn_id = "tsystems_replicon_replicon.admin"

# Sumo Logic connections
sumo_conn_id = f"tsystems_clock_export_sumo_{instance}"
dagrun_log_sumo_conn_id = "sumologic-dagrunlogger"

# Version for multiple implementations
version = ""  # _v1, _v2 etc.

dag_id_suffix = f"{instance}{version}"

# DAG IDs
master_dagid = f"tsystems_clock_in_out_time_export_master_{dag_id_suffix}"
process_clock_data_child = f"tsystems_clock_in_out_time_export_child_{dag_id_suffix}"

# Variable names
can_run_batch_task_var = f"tsystems_clock_export_can_run_batch_task_{dag_id_suffix}_var"
can_post_to_client_api_var = f"tsystems_clock_export_can_post_to_client_api_{dag_id_suffix}_var"
