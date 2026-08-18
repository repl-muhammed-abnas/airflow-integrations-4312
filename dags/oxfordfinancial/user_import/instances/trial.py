# pylint: disable=wildcard-import unused-wildcard-import
from oxfordfinancial.user_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'oxfordfinancialafmig'

replicon_conn_id = 'oxfordfinancialafmig-replicon-admin1'
sftp_conn_id = 'sftp_useast2'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'oxfordfinancial_user_import_{instance}_can_run_batch_task'
disabled = True
