# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.user_import_v1.config import *

instance = "trial"

environment = "pre-production"

company_key = "mammoettrial01trial01"

replicon_conn_id = "mammoettrial01trial01_replicon_admin"

mammoet_user_import_bearer_token_variable = "mammoet_user_import_bearer_token_variable_trial"

user_import_master_dag_id = f"mammoet_user_import_master_webhook_{instance}"
user_import_process_payload_child_dag_id = f"mammoet_user_import_process_payload_child_{instance}_v4"

disabled=True
