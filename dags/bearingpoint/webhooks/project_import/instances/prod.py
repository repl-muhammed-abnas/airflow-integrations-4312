# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.webhooks.project_import.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "bearingpointgmbh"

replicon_conn_id = "bearingpointgmbh_replicon_admin"

bearingpoint_project_import_bearer_token_variable = f"bearingpoint_project_import_bearer_token_variable_{instance}"

master_dag_id = f"bearingpoint_project_import_master_{instance}"
process_payload_dagid = f'bearingpoint_project_import_process_payload_child_{instance}_v1'
