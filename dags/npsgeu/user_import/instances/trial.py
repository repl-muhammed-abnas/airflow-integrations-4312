# pylint: disable=wildcard-import unused-wildcard-import
from npsgeu.user_import.config import *
region = 'eu-central-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'npsgeuafmig'
replicon_conn_id = 'npsgeuafmig_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

time_zone = 'America/Denver'
schedule_interval = '0 22 * * *'

max_active_runs_child = 5
max_active_runs_skills = 15

sftp_conn_id = 'sftp_useast2'
enabled_users_report= 'Enabled Users - For Integration'

input_filepath = '/npsgeu/user_import'
reference_filepath = '/npsgeu/user_import/reference/'
log_filepath = '/npsgeu/user_import/logs/'
archive_filepath = '/npsgeu/user_import/archive/'

can_run_batch_task = f'npsgeu_user_import_can_run_batch_task_{instance}'

disabled=True