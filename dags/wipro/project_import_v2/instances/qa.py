# pylint: disable=wildcard-import unused-wildcard-import
from wipro.project_import_v2.config import *

instance = "qa"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

wipro_project_task_allocation_bearer_token_variable = f"wipro_project_sync_bearer_token_variable_{instance}"
projects_child_dag_id = f"wipro_project_sync_process_payload_child_{instance}_v2"
process_project_dag_id = f"wipro_project_sync_process_each_project_child_{instance}_v2"
log_master_dag_id = f"wipro_project_sync_process_log_generation_master_{instance}_v2"

lookup_log_timestamp_var = f'wipro_project_sync_lookup_log_timestamp_{instance}'
