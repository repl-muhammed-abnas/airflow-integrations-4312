# pylint: disable=wildcard-import unused-wildcard-import
from technicolorg3.user_import.config import *
from technicolorg3.user_import.mappers.user_master_mapper_trial import user_master_mapper_trial


instance = 'trial'
environment = 'pre-production'
company_key = 'technicolorg3afmig'

replicon_conn_id = 'replicon-technicolorg3afmig-admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

user_master_mapper = user_master_mapper_trial

can_run_batch_task_var_name = f'technicolorg3_user_import_{instance}_can_run_batch_task'

disable=True

disabled=True
