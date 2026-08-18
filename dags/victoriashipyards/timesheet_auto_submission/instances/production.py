# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.timesheet_auto_submission.config import *

instance = 'production'
environment = 'production'

company_key = 'VictoriaShipyards'

replicon_conn_id = 'VictoriaShipyards-replicon-repliconint'

tenant_email = "devesh.sharma@seaspan.com,stephanie.lefort@seaspan.com,ian.gariepy@seaspan.com,andrii.perun@seaspan.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
