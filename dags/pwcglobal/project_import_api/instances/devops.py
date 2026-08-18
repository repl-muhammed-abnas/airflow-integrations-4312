# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_api.config import *
from pwcglobal.project_import_api.mapper.pwc_global_access_scope_mapper_qa import access_scope_mapper_qa

instance = 'devops'
region = 'us-west-2'
environment = 'devops'

company_key = 'pwcqaafmig'
replicon_conn_id = 'pwcqaafmig-replicon-eu.automation'

webhook_secret = f'pwc_project_import_webhook_{instance}_secret'

sftp_conn_id = 'sftp_pwc_userimport'

project_import_log_name = f'project_import_final_logs_{instance}'
log_filepath = "/PwCGlobal/Project_Import/logs"

can_redirect_to_workato_var_name = f'pwc_project_import_webhook_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_project_import_webhook_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_project_import_webhook_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_project_import_{instance}_can_run_batch_task'

lookup_log_timestamp_var = f'pwc_project_import_{instance}_lookup_log_timestamp'

pwc_global_access_scope_mapper = access_scope_mapper_qa
