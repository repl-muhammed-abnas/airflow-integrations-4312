# pylint: disable=wildcard-import unused-wildcard-import
from americanintegrated.user_sync.config import *

environment = 'pre-production'
instance = "americanIntegratedafmig"
company_key = 'americanIntegratedafmig'
replicon_conn_id = 'americanIntegratedafmig_User_Import'
can_run_batch_task_var_name = f'americanIntegratedafmig_user_import_can_run_batch_task_{instance}'
sftp_conn_id = "sftp_useast2"
input_filepath = "/americanIntegratedafmig/input"
archive_filepath = "/americanIntegratedafmig/Archive"
log_filepath = "/americanIntegratedafmig/Logs"
execution_timeout_days = 14
child_dag_max_active_runs = 2
master_dag_interval = 30
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
pacific_timezone = 'US/Pacific'
referance_filepath = "/americanIntegratedafmig/reference"

user_import_master = f'american_integrated_user_import_master_{instance}'
user_import_update_child = f'american_integrated_user_update_child_{instance}'
user_import_add_child = f'american_integrated_user_add_child_{instance}'

disabled=True
