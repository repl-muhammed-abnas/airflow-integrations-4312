# pylint: disable=wildcard-import unused-wildcard-import
from wipro.project_user_assignment_v2.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

tenant_email = "replicon.log.ext@wipro.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

wipro_project_task_allocation_bearer_token_variable = "wipro_project_task_allocation_bearer_token_variable_prod"
projects_child_dag_id = f"wipro_project_user_assignment_import_process_payload_child_{instance}_v2"
process_project_dag_id = f"wipro_project_user_assignment_import_process_each_project_child_{instance}_v2"
disable_project_master_dag_id = f"wipro_project_user_assignment_import_disable_project_master_{instance}_v2"
log_master_dag_id = f"wipro_project_user_assignment_import_process_log_generation_master_{instance}_v2"
project_dates_update = f"wipro_project_user_assignment_import_project_dates_update_master_{instance}_v2"
project_dates_update_child = f"wipro_project_user_assignment_import_project_dates_update_child_{instance}_v2"

lookup_log_timestamp_var = f'wipro_project_user_assignment_import_lookup_log_timestamp_{instance}'
