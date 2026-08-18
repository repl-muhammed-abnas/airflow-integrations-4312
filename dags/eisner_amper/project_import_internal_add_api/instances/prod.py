# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_internal_add_api.config import *

instance = 'production'
environment = 'production'

company_key = 'EisnerAmper'

replicon_conn_id = "eisneramper_repliconint.projectimport"
sftp_conn_id = 'sftp_eisneramper_521759'

log_filepath = "/Production/Project Import/Internal Project Log/Add"
exception_filepath = "/Production/Project Import/Internal Project Log/Error Log"

tenant_email = 'ashwin.ns@infosys.com,sap.alert.replicon@eisneramper.com,sap.integration.support@eisneramper.com,sap.proserv.support@eisneramper.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearer_token_var = f'eisneramper_project_import_internal_secret_{instance}'

can_run_batch_task_var_name = f'eisner_amper_project_import_internal_api_add_run_batch_task_{instance}'

master_dagid = f'eisner_amper_project_import_api_add_internal_records_master_{instance}'
process_each_client = f'eisner_amper_project_import_api_add_internal_records_process_client_child_{instance}'
process_each_project = f'eisner_amper_project_import_api_add_internal_records_process_project_child_{instance}'
process_log_generation = f'eisner_amper_project_import_api_add_internal_records_log_generation_child_{instance}'

tenant_wide_log_name = f"eisner_amper_project_import_internal_api_add_tenant_wide_log_{instance}"

sort_task_master_dagid = f'eisner_amper_project_import_api_add_internal_sort_tasks_master_{instance}'
sort_task_child_dagid = f'eisner_amper_project_import_api_add_internal_sort_tasks_child_{instance}'
