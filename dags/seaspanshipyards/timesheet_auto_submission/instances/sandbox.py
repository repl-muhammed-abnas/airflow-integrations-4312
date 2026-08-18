# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.timesheet_auto_submission.config import *

instance = 'sandbox'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'Seaspanshipyardssb'

replicon_conn_id = 'seaspanshipyardssb_replicon_rnadmin'

tenant_email = "keerthanahr@deltek.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_process_batch_task = f"seaspanshipyards_timesheet_auto_submission_can_run_batch_task_{instance}"
