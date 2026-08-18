# pylint: disable=wildcard-import unused-wildcard-import
from wipro.project_task_allocation_import.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

tenant_email = "replicon.log.ext@wipro.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

wipro_project_task_allocation_bearer_token_variable = "wipro_project_task_allocation_bearer_token_variable_prod"
project_master_dag_id = f"wipro_project_task_allocation_import_master_{instance}_v1"
projects_child_dag_id = f"wipro_project_import_process_each_empid_child_{instance}"
process_project_dag_id = f"wipro_project_import_process_each_project_child_{instance}"
disable_project_master_dag_id= f"wipro_project_import_disable_project_master_{instance}"
project_dates_update = f"wipro_process_project_start_and_enddates_{instance}"
log_master_dag_id = f"wipro_project_import_process_log_generation_master_{instance}"
disable_foreign_manager_dag_id = f'wipro_disable_foreign_supervisor_master_{instance}'
project_dates_update_child = f"wipro_process_project_start_and_enddates_child_{instance}"

lookup_log_timestamp_var = f'wipro_project_import_lookup_log_timestamp_{instance}'
