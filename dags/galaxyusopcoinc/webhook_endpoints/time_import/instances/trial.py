# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.webhook_endpoints.time_import.config import *
from galaxyusopcoinc.time_data_import_v1.instances.trial import TOTAL_BATCHES

instance = "trial"
environment = "pre-production"

company_key = "galaxyusopcoinctrial01"
replicon_conn_id = "galaxyusopcoinctrial01_replicon_admin"

# DAG IDs
master_dag_id = f"galaxyusopcoinc_time_import_webhook_master_{instance}"
child_dag_id = f"galaxyusopcoinc_timedata_import_process_payload_child_{instance}_v1"

# Token variables
galaxyusopcoinc_time_import_bearer_token_var = f"galaxyusopcoinc_time_import_bearer_token_variable_{instance}"

TOTAL_BATCHES = TOTAL_BATCHES
