# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.project_import_v1.config import *

instance = "sandbox"

environment = "pre-production"

company_key = "BearingPointSandbox"

replicon_conn_id = "BearingPointSandbox_replicon_admin"

http_conn_id = f"bearingpoint_project_import_http_logs_api_{instance}"

tenant_email = 'georgia.vasiliu@bearingpoint.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

process_payload_dagid = f'bearingpoint_project_import_process_payload_child_{instance}_v1'
client_child_dag_id = f'bearingpoint_project_import_process_clients_child_{instance}_v1'
process_log_dag_id = f'bearingpoint_project_import_process_logs_child_{instance}_v1'
process_project_dag_id = f'bearingpoint_project_import_process_each_projects_child_{instance}_v1'

can_run_batch_task_var_name = f'bearingpoint_project_import_batch_task_var_{instance}'
token_var = "bearingpoint_token_variable_uat"
