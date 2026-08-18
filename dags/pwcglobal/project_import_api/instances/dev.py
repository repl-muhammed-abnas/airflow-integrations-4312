# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_api.config import *
from pwcglobal.project_import_api.mapper.pwc_global_access_scope_mapper_qa import access_scope_mapper_qa

instance = 'dev'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.projectimport'

webhook_secret = f'pwc_project_import_webhook_{instance}_secret'

tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

project_import_log_name = f'project_import_final_logs_{instance}'
log_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Project/_logs'

can_redirect_to_workato_var_name = f'pwc_project_import_webhook_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_project_import_webhook_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_project_import_webhook_{instance}_workato_api_token'

lookup_log_timestamp_var = f'pwc_project_import_{instance}_lookup_log_timestamp'
can_run_batch_task_var_name = f'pwc_project_import_{instance}_can_run_batch_task'
pwc_global_access_scope_mapper = access_scope_mapper_qa
disabled=True
