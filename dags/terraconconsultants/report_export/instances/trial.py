# pylint: disable=wildcard-import unused-wildcard-import
from terraconconsultants.report_export.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'terracontest'
replicon_conn_id = 'terraconconsultantsafmig_replicon_admin'
webhook_secret = 'airflow_connector_ui_hmac_secret'

upload_filepath = '/terraconconsultants/extracts/'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'terraconconsultants_custom_supervisor_report_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_useast2'

disabled=True
