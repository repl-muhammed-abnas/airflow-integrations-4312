# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_api_v1.config import *
from pwcglobal.project_import_api_v1.mapper.pwc_global_access_scope_mapper import access_scope_mapper

instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'pwc'
replicon_conn_id = 'pwc-replicon-eu.projectimport'

webhook_secret = f'pwc_project_import_webhook_{instance}_secret'

tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'

project_import_log_name = f'project_import_final_logs_{instance}'
log_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Project/_logs'

can_redirect_to_workato_var_name = f'pwc_project_import_webhook_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_project_import_webhook_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_project_import_webhook_{instance}_workato_api_token'

lookup_log_timestamp_var = f'pwc_project_import_{instance}_lookup_log_timestamp'
can_run_batch_task_var_name = f'pwc_project_import_{instance}_can_run_batch_task'

pwc_global_access_scope_mapper = access_scope_mapper
project_import_api_process_payload_child_dag_id= f"pwc_project_client_process_payload_master_{instance}_v1"

dag_max_active_tasks = 10000
master_dag_max_active_runs = 20
child_dag_process_project_max_active_runs = 20
child_dag_create_project_max_active_runs = 20
child_dag_update_project_max_active_runs = 20
child_dag_log_generation_max_active_runs = 10
master_scheduled_log_generation_max_active_runs = 1
child_dag_scheduled_log_generation_max_active_runs = 5
