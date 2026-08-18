# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.report_export_ts.config import *

region = 'eu-central-1'
instance = "production"
environment = 'production'
company_key = 'pwcfr'

replicon_conn_id = 'pwcfr_replicon_automation.user'
sftp_conn_id = "sftp_pwcfr_594688"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_child = f'pwcfr_report_export_ts_child_{instance}_can_run_batch_task'

input_filepath = '/PROD/MONITORING'
