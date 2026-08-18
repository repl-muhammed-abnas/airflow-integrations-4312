# pylint: disable=wildcard-import unused-wildcard-import
from addsystems.time_entry_sync.config import *

environment = 'pre-production'
instance = "trial"
company_key = "addsystemstrial01"
bearer_token_var = 'addsystems_time_sync_trial_token'

replicon_conn_id = "ADDSystemsblanktrial_replicon_admin"
log_filepath = "/Addsystems/Time_sync/Logs"

tenant_email = "RepliconLogs@addsys.com"

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

token_var = f"{company_key}_{instance}_log_service_token"

http_conn_id = "ADDSystemsblanktrial_addsystem_log_http"

can_run_batch_task_var_name =f"{company_key}_{instance}_batch_task_var"

login_name = "admin"

disabled = True
