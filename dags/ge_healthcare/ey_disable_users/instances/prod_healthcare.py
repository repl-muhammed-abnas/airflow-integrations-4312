# pylint: disable=wildcard-import unused-wildcard-import
from ge_healthcare.ey_disable_users.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'GEHealthcare'
replicon_conn_id = 'gehealthcare_replicon_admin'
sftp_conn_id = 'ge_sftp_QWWj5RX1hdQ0'

tenant_email = "healthcare.l3.support@ge.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

schedule_interval = "0 20 28,29,30,31 * *"
time_zone = 'America/Denver'

can_run_batch_task_var_name = f'can_run_{instance}_{company_key}_batch_task_ey_disable_users'
