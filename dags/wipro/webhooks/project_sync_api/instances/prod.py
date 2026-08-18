# pylint: disable=wildcard-import unused-wildcard-import
from wipro.webhooks.project_sync_api.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_project_task_allocation_bearer_token_variable = "wipro_project_sync_bearer_token_variable_prod"
project_master_dag_id = f"wipro_project_sync_master_{instance}"
projects_child_dag_id = f"wipro_project_sync_process_payload_child_{instance}_v2"