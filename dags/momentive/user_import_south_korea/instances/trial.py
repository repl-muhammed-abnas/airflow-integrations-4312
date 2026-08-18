# pylint: disable=wildcard-import unused-wildcard-import
from momentive.user_import_south_korea.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'momentiveafmig'

replicon_conn_id = 'momentiveafmig_replicon_replicon.admin'
sftp_conn_id = 'sftp_useast2'

country = 'South Korea'

schedule_interval = '0 30 * * *'

log_filepath = '/Momentive/UserSync/SouthKorea/userimportlogs/'
archive_filepath = '/Momentive/UserSync/SouthKorea/inputarchive/'

to_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'momentive_user_import_south_korea_can_run_batch_task_{instance}'
