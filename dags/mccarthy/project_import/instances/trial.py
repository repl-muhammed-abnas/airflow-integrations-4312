# pylint: disable=wildcard-import unused-wildcard-import
from mccarthy.project_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'McCarthyafmig'
replicon_conn_id = 'mccarthyafmig_replicon_uuser'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

input_filepath = '/mccarthy/Input'
processing_filepath = '/mccarthy/Processing/'
upload_filepath = '/mccarthy/Logs/'
archive_filepath = '/mccarthy/Archive/'

can_run_batch_task_child = f'mccarthy_project_import_child_can_run_batch_task_{instance}'
disabled = True
