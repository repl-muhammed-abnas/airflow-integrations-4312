# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.timesheet_auto_submission.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'SeaspanShipyards'

replicon_conn_id = 'seaspanshipyards-replicon-admin'

tenant_email = "devesh.sharma@seaspan.com,ProdApps@seaspan.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_process_batch_task = f"seaspanshipyards_timesheet_auto_submission_can_run_batch_task_{instance}"
