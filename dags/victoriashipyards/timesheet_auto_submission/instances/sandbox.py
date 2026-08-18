# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.timesheet_auto_submission.config import *

instance = 'sandbox'
environment = 'pre-production'

company_key = 'VSLSandbox'

replicon_conn_id = 'vslsandbox-replicon-admin'

tenant_email = "devesh.sharma@seaspan.com,stephanie.lefort@seaspan.com,ian.gariepy@seaspan.com,andrii.perun@seaspan.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled=True
