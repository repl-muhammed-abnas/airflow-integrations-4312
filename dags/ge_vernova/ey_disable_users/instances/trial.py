# pylint: disable=wildcard-import unused-wildcard-import
from ge.ey_disable_users.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'GEtrial02'
replicon_conn_id = 'GEtrial02_replicon_admin'
sftp_conn_id = 'ge_sftp_QWWj5RX1hdQ0'

tenant_email = 'gesupportreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

schedule_interval = "0 20 28,29,30,31 * *"
time_zone = 'America/Denver'

can_run_batch_task_var_name = f'can_run_{instance}_{company_key}_batch_task_ey_disable_users'
disabled = True
