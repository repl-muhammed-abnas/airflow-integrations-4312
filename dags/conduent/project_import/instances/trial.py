from conduent.project_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'ConduentLLCtrial01'
replicon_conn_id = 'ConduentLLCtrial01_replicon_int'
sftp_conn_id = 'sftp_useast'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
cc_email = ""
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/conduent/Trial/Project Import/Input/'
log_filepath = '/conduent/Trial/Project Import/Logs/'
archive_filepath = '/conduent/Trial/Project Import/Archive/'


can_run_batch_task = f'conduent_project_import_can_run_batch_task_var_name_{instance}'

master_dagid = f'conduent_project_import_master_{instance}'
project_add_child_dagid = f'conduent_project_import_add_project_{instance}'
project_update_child_dagid = f'conduent_project_import_update_project_{instance}'
process_log_generation_dagid = f'conduent_project_import_process_log_generation_{instance}'
disabled = True
