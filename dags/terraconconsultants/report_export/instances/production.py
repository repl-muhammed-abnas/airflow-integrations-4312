# pylint: disable=wildcard-import unused-wildcard-import
from terraconconsultants.report_export.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'TerraconConsultants'
replicon_conn_id = 'TerraconConsultants_replicon_admin'
webhook_secret = 'airflow_connector_ui_hmac_secret'


internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'terraconconsultants_custom_supervisor_report_can_run_batch_task_{instance}'
