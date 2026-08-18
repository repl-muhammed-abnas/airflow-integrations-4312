# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.webhooks.user_import.config import *

instance = "prod"
version = "_v1"

region = 'eu-central-1'
environment = "production"

company_key = "BearingPointGmbH"

replicon_conn_id = "bearingpointgmbh_replicon_repliconint.user_import"

bearingpoint_user_import_bearer_token_variable = f"bearingpoint_user_import_bearer_token_variable_{instance}"
master_dag_id = f"bearingpoint_user_import_master_{instance}"
process_payload_child_dag_id = f"bearingpoint_user_import_process_payload_child_{instance}{version}"
