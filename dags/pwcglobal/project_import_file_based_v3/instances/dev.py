# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_file_based_v3.config import *
from pwcglobal.project_import_file_based_v3.mapper.pwc_global_access_scope_mapper_dev import access_scope_mapper_dev

instance = 'dev'
environment = 'pre-production'

company_key = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.projectimport'

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

input_filepath = '/PwCGBL_RepliconGlobal_STG/Dev/Inbound/Project/bulk_upload'
log_filepath = '/PwCGBL_RepliconGlobal_STG/Dev/Inbound/Project/bulk_upload/logs'

tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'pwc_project_import_filebased_{instance}_can_run_batch_task'
pwc_global_access_scope_mapper = access_scope_mapper_dev

pwc_project_client_master_flat_file_based_processed_file_data_variable = f"pwc_project_client_master_flat_file_based_processed_file_data_{instance}_v3"
