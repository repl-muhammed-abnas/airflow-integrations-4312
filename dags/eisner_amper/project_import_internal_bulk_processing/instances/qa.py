# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_internal_bulk_processing.config import *

instance = 'qa'
environment = 'pre-production'

company_key = 'eisnerampertrial01'

replicon_conn_id = "eisnerampertrial01_replicon_radmin"
sftp_conn_id = 'sftp_useast2'

input_filepath = "/Trial02/Project Import/Internal Project Input"
archive_filepath = "/Trial02/Project Import/Internal Project Archive"
log_filepath = "/Trial02/Project Import/Internal Project Log"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearer_token_var = f'eisneramper_project_import_internal_secret_{instance}'

can_run_batch_task_var_name = f'eisner_amper_project_import_internal_bulk_processing_run_batch_task_{instance}'

master_dagid = f'eisner_amper_project_import_bulk_processing_internal_records_master_{instance}'
process_each_client = f'eisner_amper_project_import_bulk_processing_internal_records_process_client_child_{instance}'
process_each_project = f'eisner_amper_project_import_bulk_processing_internal_records_process_project_child_{instance}'
process_log_generation = f'eisner_amper_project_import_bulk_processing_internal_records_log_generation_child_{instance}'

tenant_wide_log_name = f"eisner_amper_project_import_internal_bulk_processing_tenant_wide_log_{instance}"

sort_task_master_dagid = f'eisner_amper_project_import_bulk_processing_internal_sort_tasks_master_{instance}'
sort_task_child_dagid = f'eisner_amper_project_import_bulk_processing_internal_sort_tasks_child_{instance}'

disabled=True
