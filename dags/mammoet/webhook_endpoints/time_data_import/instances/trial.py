# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.webhook_endpoints.time_data_import.config import *

instance = "trial"

region = 'eu-central-1'
environment = "pre-production"

company_key = "mammoettrial01"

replicon_conn_id = "mammoettrial01_replicon_admin"
sftp_conn_id = 'sftp_useast2'

timedata_master_dag_id = f"mammoet_timedata_import_master_{instance}"
timedata_child_dag_id = f"mammoet_timedata_import_child_{instance}_v4"
mammoet_timedata_bearer_token_variable = "mammoet_timedata_bearer_token_variable_trial"

disabled=True
