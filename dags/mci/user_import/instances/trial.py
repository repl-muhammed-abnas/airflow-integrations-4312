# pylint: disable=wildcard-import unused-wildcard-import
from mci.user_import.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'MCIafmig'
replicon_conn_id = 'mciafmig_replicon_admin'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

input_filepath = '/mci/Input'
reference_filepath = '/mci/Reference'
log_filepath = '/mci/Logs/'
archive_filepath = '/mci/Archive/'

can_run_batch_task_child = f'mci_user_import_can_run_batch_task_child_{instance}'
disabled = True
