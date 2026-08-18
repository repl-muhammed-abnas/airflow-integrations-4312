# pylint: disable=wildcard-import unused-wildcard-import
from raynetsas.webhook_endpoints.user_import.config import *

instance = "uat"
environment = "pre-production"

company_key = "Raynetsastrial01"
replicon_conn_id = "Raynetsastrial01_replicon_admin"

raynetsas_user_import_bearer_token_variable = f"raynetsas_user_import_bearer_token_variable_{instance}"

master_dag_id = f"raynetsas_user_import_master_{instance}"
process_user_import_payload_dagid = f"raynetsas_user_import_child_{instance}"
