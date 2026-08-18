# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_api_v2.config import *
from pwcglobal.project_import_api_v2.mapper.pwc_global_access_scope_mapper_qa import access_scope_mapper_qa

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.userimport'


webhook_secret = f'pwc_project_import_webhook_{instance}_secret'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = "sftp_useast2"

project_import_log_name = f'project_import_final_logs_{instance}'
log_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Project/_logs'

can_redirect_to_workato_var_name = f'pwc_project_import_webhook_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_project_import_webhook_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_project_import_webhook_{instance}_workato_api_token'

lookup_log_timestamp_var = f'pwc_project_import_{instance}_lookup_log_timestamp'
can_run_batch_task_var_name = f'pwc_project_import_{instance}_can_run_batch_task'
pwc_global_access_scope_mapper = access_scope_mapper_qa
project_import_api_process_payload_child_dag_id= f"pwc_project_client_process_payload_master_{instance}_v2"
disabled=True