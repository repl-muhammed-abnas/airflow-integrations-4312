# pylint: disable=wildcard-import unused-wildcard-import
from moodys.timeoff_import.config import *

instance = "uat"

company_key = 'moodysemeatrial02'
replicon_conn_id = 'replicon_moodysemeatrial02_Deepak'
pgp_conn_id = 'pgp_moodysemeatrial02_timeoffsync'

sftp_conn_id = 'sftp_moodysemeatrial02_654601'

input_filepath = '/MoodysEMEA/UAT/Timeoffsync/Input'
archive_filepath = '/MoodysEMEA/UAT/Timeoffsync/Archive'
log_filepath = '/MoodysEMEA/UAT/Timeoffsync/Logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_name = f'moodys_timeoff_import_batch_run_{instance}'

disable=True

disabled=True
