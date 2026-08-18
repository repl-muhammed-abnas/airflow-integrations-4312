# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_file_based.config import *
from pwcglobal.project_import_file_based.mapper.pwc_global_access_scope_mapper import access_scope_mapper

instance = 'prod_clone1'
environment = 'production'

company_key = 'pwc'
replicon_conn_id = 'pwc-replicon-eu.projectimport'

sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'

input_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Project/Bulk_Upload'
log_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Project/Bulk_Upload/Logs'

tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'pwc_project_import_filebased_{instance}_can_run_batch_task'
pwc_global_access_scope_mapper = access_scope_mapper

pwc_project_client_master_flat_file_based_processed_file_data_variable = f"pwc_project_client_master_flat_file_based_processed_file_data_{instance}"
schedule_interval = None