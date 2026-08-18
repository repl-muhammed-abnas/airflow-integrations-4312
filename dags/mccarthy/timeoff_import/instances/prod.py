# pylint: disable=wildcard-import unused-wildcard-import
from mccarthy.timeoff_import.config import *

instance = 'prod'
environment = 'production'
company_key = 'McCarthy'
replicon_conn_id = 'mccarthy_replicon_uuser'
tenant_email = 'ABhaskerKumar@McCarthy.com,SBuerk@mccarthy.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_mccarthy_524959'

input_filepath = '/Gen3Production/TimeOffPolicyImport/Input'
log_filepath = '/Gen3Production/TimeOffPolicyImport/Logs'
archive_filepath = '/Gen3Production/TimeOffPolicyImport/Archive'

can_run_batch_task_var_name = f'mccarthy_timeoff_import_{instance}_can_run_batch_task'
