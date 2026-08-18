# pylint: disable=wildcard-import unused-wildcard-import
from npsg.user_import.config import *
from npsg.user_import.mappers.npsg_permission_mapper_mapper import npsg_permission_mapper

instance = 'trial'
environment = 'pre-production'
company_key = 'npsgafmig'
replicon_conn_id = 'npsgafmig_replicon_admin'

time_zone = 'America/Denver'
schedule_interval = '0 22 * * *'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'
enabled_users_report= 'Enabled Users - For Integration'

max_active_runs_child = 5

input_filepath = '/npsg/user_import'
reference_filepath = '/npsg/user_import/reference/'
log_filepath = '/npsg/user_import/logs/'
archive_filepath = '/npsg/user_import/archive/'

permission_mapper = npsg_permission_mapper

can_run_batch_task = f'npsg_user_import_can_run_batch_task_{instance}'

disabled=True
