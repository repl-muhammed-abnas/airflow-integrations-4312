# pylint: disable=wildcard-import unused-wildcard-import
from wipro.webhooks.project_import.config import *

instance = "qa"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

wipro_project_task_allocation_bearer_token_variable = f"wipro_project_task_allocation_bearer_token_variable_{instance}"
project_master_dag_id = f"wipro_project_task_allocation_import_master_{instance}"
projects_child_dag_id = f"wipro_project_user_assignment_import_process_payload_child_{instance}_v2"
