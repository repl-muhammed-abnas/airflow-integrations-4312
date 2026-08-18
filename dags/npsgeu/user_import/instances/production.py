# pylint: disable=wildcard-import unused-wildcard-import
from npsgeu.user_import.config import *
region = 'eu-central-1'
instance = 'production'
environment = 'production'
company_key = 'NPSGEU'
replicon_conn_id = 'npsgeu_replicon_shakeel'

tenant_email = 'replicon@npsgglobal.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

time_zone = 'America/Denver'
schedule_interval = '0 22 * * *'

max_active_runs_child = 5
max_active_runs_skills = 15

sftp_conn_id = 'sftp_npsgeu_610439'
enabled_users_report= 'Enabled Users - For Integration'

input_filepath = '/User Sync/NPSG EU/PROD'
reference_filepath = '/User Sync/NPSG EU/PROD/Reference/'
log_filepath = '/User Sync/NPSG EU/PROD/Log/'
archive_filepath = '/User Sync/NPSG EU/PROD/Archive/'

can_run_batch_task = f'npsgeu_user_import_can_run_batch_task_{instance}'
