#pylint: disable=wildcard-import unused-wildcard-import
from necau.delete_not_submitted_timeoffs.config import *
instance = 'production'

region = 'eu-central-1'
environment = 'production'

company_key = 'NECAU'
replicon_conn_id = 'necau-replicon-admin'

tenant_email = 'necau.timeoffdelete@replicon.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'necau_timeoff_delete_can_run_batch_task_{instance}'
