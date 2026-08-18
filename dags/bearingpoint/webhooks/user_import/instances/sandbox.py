# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.webhooks.user_import.config import *

instance = "sandbox"
version = "_v1"

region = 'eu-central-1'
environment = "pre-production"

company_key = "BearingPointSandbox"

replicon_conn_id = "bearingpointsandbox_replicon_repliconint_user_import"

bearingpoint_user_import_bearer_token_variable = f"bearingpoint_user_import_bearer_token_variable_{instance}"
master_dag_id = f"bearingpoint_user_import_master_{instance}"
process_payload_child_dag_id = f"bearingpoint_user_import_process_payload_child_{instance}{version}"
