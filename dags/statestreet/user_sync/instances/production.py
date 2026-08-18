# pylint: disable=wildcard-import unused-wildcard-import
from statestreet.user_sync.config import *

region = 'eu-central-1'
instance = "production"
environment = 'production'
company_key = 'statestreet'

replicon_conn_id = 'statestreet_replicon_admin'
sftp_conn_id = "sftp_statestreet_138148"
sftp_client_conn_id ="sftp_statestreet_client"

input_filepath = '/Input'
archive_filepath = '/Archive/'
log_filepath = '/Logs'

userimport_log_filepath = '/User Import/Logs'
userimport_archive_filepath = '/User Import/Archive/'
reference_filepath = '/User Import/Reference'


tenant_email = "RepliconAIS@StateStreet.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_child = f'statestreet_user_sync_add_user_child_{instance}_can_run_batch_task'
