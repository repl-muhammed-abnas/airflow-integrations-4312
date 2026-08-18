# pylint: disable=wildcard-import unused-wildcard-import
from npsg.timeoff_import.config import *

instance = 'prd'
environment = 'production'
company_key = 'npsg'
replicon_conn_id = 'npsg_replicon_admin'
sftp_conn_id = 'sftp_npsg_610439'

input_filepath = '/Time Off Sync/PROD'
archive_filepath = '/Time Off Sync/PROD/Archive'
log_filepath = '/Time Off Sync/PROD/Logs'

tenant_email = 'replicon@npsgglobal.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'npsg_timeoff_import_{instance}_can_run_batch_task'
