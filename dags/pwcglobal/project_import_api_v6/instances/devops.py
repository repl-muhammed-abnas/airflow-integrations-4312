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

lookup_log_timestamp_var = f'pwc_project_import_{instance}_lookup_log_timestamp'
pwc_global_access_scope_mapper = access_scope_mapper_qa

child_dag_scheduled_log_generation_max_active_runs = 5
master_scheduled_log_generation_max_active_runs = 1

version = 'v6'

project_import_api_process_payload_child_dag_id= f"pwc_project_client_process_payload_master_{instance}_{version}"

project_import_api_create_project_child_dag_id = f'pwc_project_import_child_create_project_b1_{instance}_{version}'
project_import_api_log_generation_child_dag_id = f'pwc_project_import_child_log_pregeneration_{instance}_{version}'
project_import_api_process_project_child_dag_id = f'pwc_project_import_child_process_project_b1_{instance}_{version}'
project_import_api_update_project_child_dag_id = f'pwc_project_import_child_update_project_b1_{instance}_{version}'
project_import_api_log_child_dag_id = f'pwc_project_import_child_log_{instance}_{version}'
project_import_api_log_schedule_master_dag_id = f'pwc_project_import_master_log_scheduled_{instance}_{version}'
