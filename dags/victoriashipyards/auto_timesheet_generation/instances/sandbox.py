# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.auto_timesheet_generation.config import *

instance = 'sandbox'
environment = 'pre-production'

company_key = 'seaspanvslsb'

replicon_conn_id = 'seaspanvslsb_replicon_repliconint'

tenant_email = "keerthanahr@deltek.com"
internal_logs_email = ""
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
