# pylint: disable=wildcard-import unused-wildcard-import
from wipro.webhooks.project_sync_api.config import *

instance = "trial"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

tenant_email = "replicon.log.ext@wipro.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

wipro_project_task_allocation_bearer_token_variable = "wipro_project_sync_bearer_token_variable_prod"
project_master_dag_id = f"wipro_project_sync_master_{instance}"
projects_child_dag_id = f"wipro_project_sync_process_payload_child_{instance}_v2"
