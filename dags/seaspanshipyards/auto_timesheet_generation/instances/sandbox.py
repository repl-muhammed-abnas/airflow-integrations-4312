# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_timesheet_generation.config import *

instance = 'sandbox'
environment = 'pre-production'

company_key = 'Seaspanshipyardssb'

replicon_conn_id = 'seaspanshipyardssb_replicon_rnadmin'

tenant_email = "keerthanahr@deltek.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
