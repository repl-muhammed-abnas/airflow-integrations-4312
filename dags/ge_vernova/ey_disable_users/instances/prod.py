# pylint: disable=wildcard-import unused-wildcard-import
from ge.ey_disable_users.config import *

instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'GEVernova'
replicon_conn_id = 'GEVernova_replicon_admin'
sftp_conn_id = 'GEVernova_sftp_li013214sd-REPLICON'

tenant_email = 'gesupportreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

schedule_interval = "0 20 28,29,30,31 * *"
time_zone = 'America/Denver'

can_run_batch_task_var_name = f'can_run_{instance}_{company_key}_batch_task_ey_disable_users'
