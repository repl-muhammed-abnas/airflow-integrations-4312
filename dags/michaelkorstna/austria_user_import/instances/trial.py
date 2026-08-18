# pylint: disable=wildcard-import unused-wildcard-import
from michaelkorstna.austria_user_import.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'Michaelkorstnaafmig'
replicon_conn_id = 'michaelkorstnaafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

schedule_interval = '0 22 * * *'

max_active_runs_groups=1 #We cannot increase this
max_active_runs_child=5

time_zone = 'Etc/UTC'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_support_email_cc = '{{ var.value.dagrun_internal_testing_email }}'

reference_filepath = '/michaelkorstna/reference/'
archive_filepath = '/michaelkorstna/archives/'


can_run_batch_task = f'michaelkorstna_austria_user_import_can_run_batch_task_{instance}'

disabled=True
