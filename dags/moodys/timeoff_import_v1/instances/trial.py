# pylint: disable=wildcard-import unused-wildcard-import
from moodys.timeoff_import_v1.config import *

instance = "trial"
version = "v1"

company_key = 'moodysemeatrial03'
replicon_conn_id = 'replicon_moodysemeatrial03_Deepak'
pgp_conn_id = 'pgp_moodys_internal_testing_timeoffsync'

sftp_conn_id = 'rsftp-useast_for_testing'

input_filepath = '/MoodysEMEATrial03/Time Off Sync Trial 03/Input'
archive_filepath = '/MoodysEMEATrial03/Time Off Sync Trial 03/Archive'
log_filepath = '/MoodysEMEATrial03/Time Off Sync Trial 03/Logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f'moodys_timeoff_import_master_{instance}_{version}'
child_dag_id = f'moodys_timeoff_import_process_each_record_child_{instance}_{version}'

can_run_batch_task_name = f'moodys_timeoff_import_batch_run_{instance}_{version}'
