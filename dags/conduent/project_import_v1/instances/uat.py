from conduent.project_import_v1.config import *

instance = 'uat'
environment = 'pre-production'
company_key = "ConduentLLCSandbox"
replicon_conn_id = "ConduentLLCSandbox_replicon_int"
sftp_conn_id = "sftp_633276"


tenant_email = "RepliconIntegrations@conduent.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Sandbox/Project Import/Input/'
log_filepath = '/Sandbox/Project Import/Log/'
archive_filepath = '/Sandbox/Project Import/Archive/'


can_run_batch_task = f'conduent_project_import_can_run_batch_task_var_name_{instance}'

master_dagid = f'conduent_project_import_master_{instance}_v1'
project_add_child_dagid = f'conduent_project_import_add_project_{instance}_v1'
project_update_child_dagid = f'conduent_project_import_update_project_{instance}_v1'
process_log_generation_dagid = f'conduent_project_import_process_log_generation_{instance}_v1'
