# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_timesheet_generation.config import *

instance = 'production'
environment = 'production'

company_key = 'seaspanshipyards'

replicon_conn_id = 'seaspanshipyards-replicon-admin'

tenant_email = "devesh.sharma@seaspan.com,ProdApps@seaspan.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
