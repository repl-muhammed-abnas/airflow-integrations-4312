# pylint: disable=wildcard-import unused-wildcard-import
from mccarthy.user_import.config import *

region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'mccarthyafmig'

replicon_conn_id = 'mccarthyafmig_replicon_uuser'
sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'mccarthy_user_import_{instance}_can_run_batch_task'
disabled = True
