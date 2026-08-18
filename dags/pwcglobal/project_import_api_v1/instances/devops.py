# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_api_v1.config import *
from pwcglobal.project_import_api_v1.mapper.pwc_global_access_scope_mapper_qa import access_scope_mapper_qa

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
dag_max_active_tasks = 10000
master_dag_max_active_runs = 5
child_dag_process_project_max_active_runs = 5
child_dag_create_project_max_active_runs = 5
child_dag_update_project_max_active_runs = 5
child_dag_log_generation_max_active_runs = 5

pwc_global_access_scope_mapper = access_scope_mapper_qa
project_import_api_process_payload_child_dag_id= f"pwc_project_client_process_payload_master_{instance}_v1"

disabled = True
