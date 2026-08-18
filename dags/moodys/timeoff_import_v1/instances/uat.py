# pylint: disable=wildcard-import unused-wildcard-import
from moodys.timeoff_import_v1.config import *

instance = "uat"
version = "v1"

company_key = 'moodysemeatrial03'
replicon_conn_id = 'replicon_moodysemeatrial03_Deepak'
pgp_conn_id = 'pgp_moodysemeatrial02_timeoffsync'

sftp_conn_id = 'sftp_moodysemea_654601'

input_filepath = '/MoodysEMEA/UAT/Timeoffsync/Input'
archive_filepath = '/MoodysEMEA/UAT/Timeoffsync/Archive'
log_filepath = '/MoodysEMEA/UAT/Timeoffsync/Logs'

tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f'moodys_timeoff_import_master_{instance}_{version}'
child_dag_id = f'moodys_timeoff_import_process_each_record_child_{instance}_{version}'

can_run_batch_task_name = f'moodys_timeoff_import_batch_run_{instance}_{version}'
