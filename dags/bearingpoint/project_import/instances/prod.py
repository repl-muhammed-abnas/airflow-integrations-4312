# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.project_import.config import *

instance = "prod"

environment = "production"

company_key = "bearingpointgmbh"

replicon_conn_id = "bearingpointgmbh_replicon_admin"

http_conn_id = f"bearingpoint_project_import_http_logs_api_{instance}"

tenant_email = 'georgia.vasiliu@bearingpoint.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

process_payload_dagid = f'bearingpoint_project_import_process_payload_child_{instance}'
client_child_dag_id = f'bearingpoint_project_import_process_clients_child_{instance}'
process_log_dag_id = f'bearingpoint_project_import_process_logs_child_{instance}'
process_project_dag_id = f'bearingpoint_project_import_process_each_projects_child_{instance}'

can_run_batch_task_var_name = f'bearingpoint_project_import_batch_task_var_{instance}'
token_var = "bearingpoint_token_variable_prod"
