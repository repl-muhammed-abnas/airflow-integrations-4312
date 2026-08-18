# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.auto_timesheet_generation.config import *

instance = 'sandbox2'
environment = 'pre-production'

company_key = 'SeaspanShipyardsOra'

replicon_conn_id = 'seaspanshipyardsora_replicon_rnadmin'

tenant_email = "keerthanahr@deltek.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
