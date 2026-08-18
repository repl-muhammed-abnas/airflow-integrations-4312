# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.time_data_import_v1.config import *

instance = "prod"

region = 'us-east-1'
environment = "production"

company_key = "GalaxyUSOpcoInc"

replicon_conn_id = "galaxyusopcoinc_replicon_timeimport"

# Version
version = "_v1"
dag_suffix = f"{instance}{version}"

# DAG IDs
timedata_child_dag_id = f"galaxyusopcoinc_timedata_import_process_payload_child_{dag_suffix}"

# Customer endpoint connection
customer_log_endpoint_conn_id = f"galaxyusopcoinc_time_data_log_endpoint_{instance}"
can_run_batch_task_var_name = f"galaxyusopcoinc_time_data_import_can_run_batch_task_{instance}"
can_post_log_to_customer_var_name = f"galaxyusopcoinc_time_data_import_can_post_log_to_customer_{instance}"

# Execution settings
child_max_active_run = 3
USERS_COUNT = 15
BATCHES_PER_USER = 5
TOTAL_BATCHES = USERS_COUNT * BATCHES_PER_USER