# pylint: disable=wildcard-import unused-wildcard-import
from technicolorg3.user_import.config import *
from technicolorg3.user_import.mappers.user_master_mapper_prod import user_master_mapper_prod


instance = 'production'
environment = 'production'
company_key = 'technicolorg3'

replicon_conn_id = 'replicon-technicolorG3-admin'

tenant_email = 'psadvreplicon-support@technicolor.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'technicolorg3-sftp-ps_nemor01'

user_master_mapper = user_master_mapper_prod

can_run_batch_task_var_name = f'technicolorg3_user_import_{instance}_can_run_batch_task'
disabled = True
