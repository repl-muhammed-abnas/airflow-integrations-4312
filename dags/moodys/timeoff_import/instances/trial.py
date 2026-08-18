# pylint: disable=wildcard-import unused-wildcard-import
from moodys.timeoff_import.config import *

instance = "trial"

company_key = 'moodysemeatrial03'
replicon_conn_id = 'replicon_moodysemeatrial03_Deepak'
pgp_conn_id = 'pgp_moodysemeatrial02_timeoffsync'

sftp_conn_id = 'sftp_trail_654601_moodysemeatrial03'

input_filepath = '/Time Off Sync Trial 03/Input'
archive_filepath = '/Time Off Sync Trial 03/Archive'
log_filepath = '/Time Off Sync Trial 03/Logs'

tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_name = f'moodys_timeoff_import_batch_run_{instance}'
