# pylint: disable=wildcard-import unused-wildcard-import
from matlensilver.user_sync_integration.disable_user.config import *

instance = 'production'
environment = 'production'

company_key = 'MatlenSilver'
replicon_conn_id = 'matlensilver_replicon_admin'

master_dag_interval = '0 1 * * *'

tenant_email = 'IT@matlensilver.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
