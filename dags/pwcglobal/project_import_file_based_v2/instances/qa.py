# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_file_based_v2.config import *
from pwcglobal.project_import_file_based_v2.mapper.pwc_global_access_scope_mapper_qa import access_scope_mapper_qa

instance = 'qa'
environment = 'pre-production'

company_key = 'pwcqa'
replicon_conn_id = 'pwcqa-replicon-eu.projectimport'

sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'

input_filepath = '/PwCGBL_RepliconGlobal_STG/QA/Inbound/Project/bulk_upload'
log_filepath = '/PwCGBL_RepliconGlobal_STG/QA/Inbound/Project/bulk_upload/logs'

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'pwc_project_import_filebased_{instance}_can_run_batch_task'
pwc_global_access_scope_mapper = access_scope_mapper_qa

pwc_project_client_master_flat_file_based_processed_file_data_variable = f"pwc_project_client_master_flat_file_based_processed_file_data_{instance}_v2"

disabled=True
