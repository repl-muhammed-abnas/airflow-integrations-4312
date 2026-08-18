# pylint: disable=wildcard-import unused-wildcard-import
from dataaxle.timeoff_balance_export.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'Dataaxle'
replicon_conn_id = 'dataaxle_replicon_radmin'



internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'data_axle_custom_utilization_report_can_run_batch_task_{instance}'
