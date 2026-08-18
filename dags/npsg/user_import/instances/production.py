# pylint: disable=wildcard-import unused-wildcard-import
from npsg.user_import.config import *
from npsg.user_import.mappers.npsg_permission_mapper_mapper import npsg_permission_mapper

instance = 'production'
environment = 'production'
company_key = 'NPSG'
replicon_conn_id = 'npsg_replicon_admin'

time_zone = 'America/Denver'
schedule_interval = '0 22 * * *'

tenant_email = 'replicon@npsgglobal.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_npsg_610439'
enabled_users_report= 'Enabled Users - For Integration'

max_active_runs_child = 5

input_filepath = '/User Sync/NPSG/PROD'
reference_filepath = '/User Sync/NPSG/PROD/Reference/'
log_filepath = '/User Sync/NPSG/PROD/Log/'
archive_filepath = '/User Sync/NPSG/PROD/Archive/'

permission_mapper = npsg_permission_mapper

can_run_batch_task = f'npsg_user_import_can_run_batch_task_{instance}'
