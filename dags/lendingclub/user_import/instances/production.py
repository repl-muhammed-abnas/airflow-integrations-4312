# pylint: disable=wildcard-import unused-wildcard-import
from lendingclub.user_import.config import *
region = 'us-east-1'
environment = 'production'
instance = 'prod'
company_key = 'LendingClub'

replicon_conn_id = 'lendingclub_replicon_admin'
sftp_conn_id = 'sftp_lendingclub_admin'
pgp_conn_id = "pgp_lendingclub_userimport"

input_filepath = '/Input'

input_filepath_master = '/Processing'
log_filepath = '/Logs'
archive_filepath = '/Archive'

to_email = "WD_integrations@lendingclub.com"
bcc_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'lendingclub_user_import_{instance}_can_run_batch_task'
