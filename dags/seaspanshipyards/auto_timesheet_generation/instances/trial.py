# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_timesheet_generation.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'seaspanshipyardsafmig'

replicon_conn_id = 'seaspanshipyardsafmig-replicon-admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
