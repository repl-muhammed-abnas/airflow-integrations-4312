# pylint: disable=wildcard-import unused-wildcard-import
from ingenta.custom_report_extract.config import *

region = 'us-east-2'
instance = "trial"
environment = 'pre-production'
company_key = 'Ingentaafmig'

replicon_conn_id = 'Ingentaafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'ingenta_custom_report_extract_{instance}_can_run_batch_task'

log_filepath = '/ingenta/customexport'
