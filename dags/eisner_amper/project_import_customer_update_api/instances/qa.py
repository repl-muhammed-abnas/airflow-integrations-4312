# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.project_import_customer_update_api.config import *

instance = 'qa'
environment = 'pre-production'

company_key = 'eisnerampertrial01'

replicon_conn_id = "eisnerampertrial01_replicon_radmin"
sftp_conn_id = 'sftp_useast2'

log_filepath = "/Trial02/Project Import/Customer Project Log"
exception_filepath = "/Trial02/Project Import/Customer Project Log/Error Log"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
internal_logs ='{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'eisner_amper_project_import_customer_run_batch_task_{instance}'

master_dagid = f'eisner_amper_project_import_api_update_customer_records_master_{instance}'
process_each_client = f'eisner_amper_project_import_api_update_customer_records_process_client_child_{instance}'
process_each_project = f'eisner_amper_project_import_api_update_customer_records_process_project_child_{instance}'
process_each_task = f'eisner_amper_project_import_api_update_customer_records_process_task_child_{instance}'
process_log_generation = f'eisner_amper_project_import_api_update_customer_records_log_generation_child_{instance}'

tenant_wide_log_name = f"eisner_amper_project_import_api_update_customer_tenant_wide_log_{instance}"

sort_task_master_dagid = f'eisner_amper_project_import_api_update_customer_sort_tasks_master_{instance}'
sort_task_child_dagid = f'eisner_amper_project_import_api_update_customer_sort_tasks_child_{instance}'

disabled=True
