# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.auto_timesheet_generation.config import *

instance = 'production'
environment = 'production'

company_key = 'VictoriaShipyards'

replicon_conn_id = 'VictoriaShipyards-replicon-repliconint'

tenant_email = "ProdApps@seaspan.com"
internal_logs_email = ""
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
