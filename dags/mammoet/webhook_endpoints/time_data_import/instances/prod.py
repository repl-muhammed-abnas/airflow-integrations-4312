# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.webhook_endpoints.time_data_import.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"

timedata_master_dag_id = f"mammoet_timedata_import_master_{instance}"
timedata_child_dag_id = f"mammoet_timedata_import_child_{instance}_v4"
mammoet_timedata_bearer_token_variable = f"mammoet_timedata_bearer_token_variable_{instance}"
