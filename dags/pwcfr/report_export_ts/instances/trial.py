# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.report_export_ts.config import *

region = 'eu-central-1'
instance = "trial"
environment = 'pre-production'
company_key = 'pwcfrafmig'

replicon_conn_id = 'pwcfr_admin_replicon'
sftp_conn_id = "sftp_useast2"

schedule_interval = "0 0 * * *"
time_zone = "Europe/Paris"

can_run_batch_task_child = f'pwcfr_report_export_ts_child_{instance}_can_run_batch_task'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/PROD/MONITORING'
disabled = True
