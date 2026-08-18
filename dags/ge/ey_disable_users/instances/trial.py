# pylint: disable=wildcard-import unused-wildcard-import
from ge.ey_disable_users.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'GEafmig'
replicon_conn_id = 'GEafmig_replicon_admin'
sftp_conn_id = 'client_horizon_sftp'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

schedule_interval = "0 20 28,29,30,31 * *"
time_zone = 'America/Denver'

can_run_batch_task_var_name = f'can_run_{instance}_{company_key}_batch_task_ey_disable_users'
disabled = True
