# pylint: disable=wildcard-import unused-wildcard-import
from sunovion.user_import.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'Sunovionafmig'
replicon_conn_id = 'sunovion_replicon_payrollteam'
sftp_conn_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_support_email_cc = '{{ var.value.dagrun_internal_testing_email }}'

input_filepath = '/sunovion/processing'
archive_filepath = '/sunovion/archive/'
log_filepath = '/sunovion/logs/'


can_run_batch_task = f'sunovion_user_import_can_run_batch_task_{instance}'

disable=True

disabled=True
