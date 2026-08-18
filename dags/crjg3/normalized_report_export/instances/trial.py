# pylint: disable=wildcard-import unused-wildcard-import
from crjg3.normalized_report_export.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'crjg3afmig'
replicon_conn_id = 'crjg3afmig_replicon_admin'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'crjg3_normalized_report_export_can_run_batch_task_{instance}'

