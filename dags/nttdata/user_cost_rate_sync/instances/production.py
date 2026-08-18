# pylint: disable=wildcard-import unused-wildcard-import
from nttdata.user_cost_rate_sync.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'NTTData'
replicon_conn_id = 'nttdata_replicon_replicon'

max_active_runs_child = 5


tenant_email = 'David.Landry@nttdata.com,Justin.Terrill@nttdata.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'nttdata_sftp_618198'

input_filepath = '/CostRate'
reference_filepath = '/Reference/'
archive_filepath = '/Archive/'

can_run_batch_task = f'nttdata_user_cost_rate_sync_can_run_batch_task_{instance}'
