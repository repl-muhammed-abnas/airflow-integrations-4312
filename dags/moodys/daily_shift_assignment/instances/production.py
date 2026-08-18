# pylint: disable=wildcard-import unused-wildcard-import
from moodys.daily_shift_assignment.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'MoodysEMEA'
replicon_conn_id = 'moodysemea-replicon-admin'
sftp_conn_id = 'moodysemea_sftp_654601'

log_filepath = '/shiftassignment/logs'
reference_filepath = '/shiftassignment/reference'

can_run_batch_task_var_name = f'moodys_daily_shift_assignment_{instance}_can_run_batch_task'

tenant_email = 'Gloria.Wong@moodys.com,Justina.Uselyte@moodys.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
