# pylint: disable=wildcard-import unused-wildcard-import
from addsystems.project_import.config import *

environment = 'pre-production'
instance = "trial"
company_key = "ADDSystemsblanktrial"
bearer_token_var = 'addsystems_project_import_trial_token'

sftp_conn_id = "sftp_useast2"

replicon_conn_id = "ADDSystemsblanktrial_replicon_admin"
log_filepath = "/Addsystems/Project_import/Logs"

tenant_email = "RepliconLogs@addsys.com"

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

token_var = f"{company_key}_{instance}_log_service_token"

http_conn_id = "ADDSystemsblanktrial_addsystem_log_http"


can_run_batch_task_var_name = f'addsystems_project_sync_run_batch_task_{instance}'
disabled = True