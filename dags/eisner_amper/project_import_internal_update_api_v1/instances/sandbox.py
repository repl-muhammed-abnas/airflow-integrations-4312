# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_internal_update_api_v1.config import *

instance = 'sandbox'
version = '_v1'
environment = 'pre-production'

company_key = 'EisnerAmperSandbox'

replicon_conn_id = "eisnerampersandbox_repliconint.projectimport"
sftp_conn_id = 'sftp_eisnerampersandbox_521759'

log_filepath = "/Sandbox/Project Import/Internal Project Log/Update"
exception_filepath = '/Sandbox/Project Import/Customer Project Log/Error Log'

tenant_email = 'Amit.tiwari@eisneramper.com, Richa.sinha@eisneramper.com, sap.integration.support@eisneramper.com, sap.proserv.support@eisneramper.com'
internal_logs = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearer_token_var = f'eisneramper_project_import_internal_secret_{instance}{version}'

can_run_batch_task_var_name = f'esiner_amper_project_import_internal_can_run_batch_task_{instance}{version}'

master_dagid = f'eisner_amper_project_import_api_update_internal_records_master_{instance}{version}'
process_each_client = f'eisner_amper_project_import_api_update_internal_records_process_client_child_{instance}{version}'
process_each_project = f'eisner_amper_project_import_api_update_internal_records_process_project_child_{instance}{version}'
process_each_task = f'eisner_amper_project_import_api_update_internal_records_process_task_child_{instance}{version}'
process_log_generation = f'eisner_amper_project_import_api_update_internal_records_log_generation_child_{instance}{version}'

tenant_wide_log_name = f"eisner_amper_project_import_api_update_internal_tenant_wide_log_{instance}{version}"

sort_task_master_dagid = f'eisner_amper_project_import_api_update_internal_sort_tasks_master_{instance}{version}'
sort_task_child_dagid = f'eisner_amper_project_import_api_update_internal_sort_tasks_child_{instance}{version}'
