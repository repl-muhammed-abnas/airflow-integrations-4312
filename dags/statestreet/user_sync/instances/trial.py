# pylint: disable=wildcard-import unused-wildcard-import
from statestreet.user_sync.config import *

region = 'eu-central-1'
instance = "trial"
environment = 'pre-production'
company_key = 'Statestreetafmig'

replicon_conn_id = 'statestreetafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

sftp_client_conn_id ="sftp_eucentral"

input_filepath = '/statestreet/Input'
archive_filepath = '/statestreet/Archive/'
log_filepath = '/statestreet/Logs'

userimport_log_filepath = '/User Import/Logs'
userimport_archive_filepath = '/User Import/Archive/'
reference_filepath = '/User Import/Reference'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_child = f'statestreet_user_sync_add_user_child_{instance}_can_run_batch_task'

disable=True

disabled=True
