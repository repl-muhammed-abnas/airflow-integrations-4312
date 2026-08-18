# pylint: disable=wildcard-import unused-wildcard-import
from mccarthy.user_import.config import *

instance = 'production'
environment = 'production'
company_key = 'mccarthy'

replicon_conn_id = 'mccarthy_replicon_uuser'
sftp_conn_id = 'sftp_mccarthy_524959'

tenant_email = 'ABhaskerKumar@McCarthy.com,SBuerk@mccarthy.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'mccarthy_user_import_{instance}_can_run_batch_task'
