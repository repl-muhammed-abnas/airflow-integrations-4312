# pylint: disable=wildcard-import unused-wildcard-import
from mci.user_import.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'MCI'
replicon_conn_id = 'mci_replicon_admin'
tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_mci_replicon'

input_filepath = '/outbound'
reference_filepath = '/reference'
log_filepath = '/logs/'
archive_filepath = '/archive/'

can_run_batch_task_child = f'mci_user_import_can_run_batch_task_child_{instance}'
