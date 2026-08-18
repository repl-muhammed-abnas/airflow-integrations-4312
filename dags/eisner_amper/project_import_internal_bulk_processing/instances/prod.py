# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_internal_bulk_processing.config import *

instance = 'production'
environment = 'production'

company_key = 'EisnerAmper'

replicon_conn_id = "eisneramper_repliconint.projectimport"
sftp_conn_id = 'sftp_eisneramper_521759'

input_filepath = "/Production/Bulk Project Creation/Internal Project/Input"
archive_filepath = "/Production/Bulk Project Creation/Internal Project/Archive"
log_filepath = "/Production/Bulk Project Creation/Internal Project/Log"

# pylint: disable=line-too-long
tenant_email = 'ashwin.ns@infosys.com,sap.alert.replicon@eisneramper.com,sap.integration.support@eisneramper.com,sap.proserv.support@eisneramper.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'eisner_amper_project_import_internal_bulk_processing_run_batch_task_{instance}'

master_dagid = f'eisner_amper_project_import_bulk_processing_internal_records_master_{instance}'
process_each_client = f'eisner_amper_project_import_bulk_processing_internal_records_process_client_child_{instance}'
process_each_project = f'eisner_amper_project_import_bulk_processing_internal_records_process_project_child_{instance}'
process_log_generation = f'eisner_amper_project_import_bulk_processing_internal_records_log_generation_child_{instance}'

tenant_wide_log_name = f"eisner_amper_project_import_internal_bulk_processing_tenant_wide_log_{instance}"

sort_task_master_dagid = f'eisner_amper_project_import_bulk_processing_internal_sort_tasks_master_{instance}'
sort_task_child_dagid = f'eisner_amper_project_import_bulk_processing_internal_sort_tasks_child_{instance}'
