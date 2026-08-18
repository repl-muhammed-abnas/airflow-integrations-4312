# pylint: disable=wildcard-import unused-wildcard-import
from groupmportugal.project_sync.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'GroupMPortugalafmig'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id = 'groupmportugalafmig_replicon_admin'

master_dag = f'groupmportugal_project_sync_master_{instance}'
process_webhook_records_child_dag = f"groupmportugal_process_webhook_records_child_{instance}"
create_update_projects = f"groupmportugal_create_update_projects_child_{instance}"
add_client = f"groupmportugal_add_client_child_{instance}"

lookup_log_timestamp_var = f'groupmportugal_project_import_lookup_log_timestamp_{instance}'

can_redirect_to_workato_var_name = f'groupmportugal_project_import_{instance}_redirect_to_workato'
workato_api_endpoint = f'groupmportugal_project_import_{instance}_workato_endpoint'
can_run_batch_task_var_name = f"groupmportugal_project_import_batch_task_enabled_{instance}"

disabled=True
