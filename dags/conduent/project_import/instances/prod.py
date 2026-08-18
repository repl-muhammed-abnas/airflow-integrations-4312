from conduent.project_import.config import *

instance = 'prod'
environment = 'production'
company_key = "Conduent"
replicon_conn_id = "conduent_replicon_repliconint"
sftp_conn_id = "sftp_633276"


tenant_email = "RepliconIntegrations@conduent.com"
cc_email = "nareshmv@deltek.com,SumitTomar@deltek.com,AnishHiralikar@deltek.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Production/Project Import/Input/'
log_filepath = '/Production/Project Import/Log/'
archive_filepath = '/Production/Project Import/Archive/'


can_run_batch_task = f'conduent_project_import_can_run_batch_task_var_name_{instance}'

master_dagid = f'conduent_project_import_master_{instance}'
project_add_child_dagid = f'conduent_project_import_add_project_{instance}'
project_update_child_dagid = f'conduent_project_import_update_project_{instance}'
process_log_generation_dagid = f'conduent_project_import_process_log_generation_{instance}'
