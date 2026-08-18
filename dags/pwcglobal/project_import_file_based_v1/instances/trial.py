# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_file_based_v1.config import *
from pwcglobal.project_import_file_based_v1.mapper.pwc_global_access_scope_mapper_qa import access_scope_mapper_qa

instance = 'trial'
environment = 'pre-production'

company_key = 'pwcinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

sftp_conn_id = 'sftp_eucentral1_airflow'

input_filepath = '/PwCGlobal/Project_Import_FileBased/input'
log_filepath = '/PwCGlobal/Project_Import_FileBased/logs'

should_archive = True
if should_archive:
    archive_filepath = '/PwCGlobal/Project_Import_FileBased/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'pwc_project_import_filebased_{instance}_can_run_batch_task'
pwc_global_access_scope_mapper = access_scope_mapper_qa
disabled = True

pwc_project_client_master_flat_file_based_processed_file_data_variable = f"pwc_project_client_master_flat_file_based_processed_file_data_{instance}_v1"
