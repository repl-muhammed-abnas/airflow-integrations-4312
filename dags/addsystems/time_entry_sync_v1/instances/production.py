# pylint: disable=wildcard-import unused-wildcard-import
from addsystems.time_entry_sync_v1.config import *

environment = 'production'
instance = "production"
company_key = "ADDSystems"
bearer_token_var = 'addsystems_time_sync_prod_token'

replicon_conn_id = "ADDSystems_replicon_integration_admin"

tenant_email = "RepliconLogs@addsys.com"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

token_var = f"{company_key}_{instance}_log_service_token"

http_conn_id = "ADDSystems_log_http"

login_name = "integration_admin"
