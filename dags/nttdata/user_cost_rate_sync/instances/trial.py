# pylint: disable=wildcard-import unused-wildcard-import
from nttdata.user_cost_rate_sync.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'nttdataafmig'
replicon_conn_id = 'nttdataafmig_replicon_replicon'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

max_active_runs_child = 1

input_filepath = '/nttdata/costrate'
reference_filepath = '/nttdata/reference/'
archive_filepath = '/nttdata/archive/'

can_run_batch_task = f'nttdata_user_cost_rate_sync_can_run_batch_task_{instance}'

disable=True

disabled=True
