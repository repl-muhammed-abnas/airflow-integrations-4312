# pylint: disable=wildcard-import unused-wildcard-import
from dominos.user_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'dominospizzaafmig'

replicon_conn_id = 'replicon-dominospizzaafmig-adminr'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'
secondary_sftp_conn_id = 'sftp_useast2'

can_run_batch_task_var_name = f'dominospizza_user_import_{instance}_can_run_batch_task'

disable=True

disabled=True
