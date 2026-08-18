# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.time_data_import.config import *

instance = "prod"

region = 'us-east-1'
environment = "production"

company_key = "GalaxyUSOpcoInc"

replicon_conn_id = "galaxyusopcoinc_replicon_timeimport"

# DAG IDs
timedata_child_dag_id = f"galaxyusopcoinc_timedata_import_process_payload_child_{instance}"

# Customer endpoint connection
customer_log_endpoint_conn_id = f"galaxyusopcoinc_time_data_log_endpoint_{instance}"
can_run_batch_task_var_name = f"galaxyusopcoinc_time_data_import_can_run_batch_task_{instance}"
