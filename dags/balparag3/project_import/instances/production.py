# pylint: disable=wildcard-import unused-wildcard-import
from balparag3.project_import.config import *

instance = 'production'
environment = 'production'

company_key = 'balparag3'
replicon_conn_id = 'balparag3_replicon_Sblanche'

sftp_conn_id = 'sftp_Integration_GmailtoSFTP'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'balparag3_project_import_{instance}_can_run_batch_task'
