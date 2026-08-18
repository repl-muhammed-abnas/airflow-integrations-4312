# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.time_data_import.config import *

instance = "trial"

region = 'us-east-1'
environment = "pre-production"

company_key = "galaxyusopcoinctrial01"

replicon_conn_id = "galaxyusopcoinctrial01_replicon_timeimport"

# DAG IDs
timedata_child_dag_id = f"galaxyusopcoinc_timedata_import_process_payload_child_{instance}"

# Customer endpoint connection
customer_log_endpoint_conn_id = f"galaxyusopcoinc_time_data_log_endpoint_{instance}"
can_run_batch_task_var_name = f"galaxyusopcoinc_time_data_import_can_run_batch_task_{instance}"

USERS_COUNT = 15
BATCHES_PER_USER = 5
TOTAL_BATCHES = USERS_COUNT * BATCHES_PER_USER