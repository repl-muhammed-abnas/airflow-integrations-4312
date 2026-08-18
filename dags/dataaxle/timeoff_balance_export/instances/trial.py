# pylint: disable=wildcard-import unused-wildcard-import
from dataaxle.timeoff_balance_export.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'dataaxleafmig'
replicon_conn_id = 'dataaxleafmig_replicon_radmin'

upload_filepath = '/utilizationreport/'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'data_axle_custom_utilization_report_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_useast2'
