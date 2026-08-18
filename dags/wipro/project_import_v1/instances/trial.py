# pylint: disable=wildcard-import unused-wildcard-import
from wipro.project_import_v1.config import *

instance = "trial"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

tenant_email = "replicon.log.ext@wipro.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

wipro_project_task_allocation_bearer_token_variable = "wipro_project_task_allocation_bearer_token_variable_trial"
projects_child_dag_id = f"wipro_project_import_process_payload_child_{instance}_v1"
process_project_dag_id = f"wipro_project_import_process_each_project_child_{instance}_v1"
disable_project_master_dag_id= f"wipro_project_import_disable_project_master_{instance}_v1"
project_dates_update = f"wipro_process_project_start_and_enddates_{instance}_v1"
log_master_dag_id = f"wipro_project_import_process_log_generation_master_{instance}_v1"
disable_foreign_manager_dag_id = f'wipro_disable_foreign_supervisor_master_{instance}_v1'
project_dates_update_child = f"wipro_process_project_start_and_enddates_child_{instance}_v1"

lookup_log_timestamp_var = f'wipro_project_import_lookup_log_timestamp_{instance}'

disabled=True
