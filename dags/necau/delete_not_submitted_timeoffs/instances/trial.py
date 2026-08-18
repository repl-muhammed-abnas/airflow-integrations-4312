#pylint: disable=wildcard-import unused-wildcard-import
from necau.delete_not_submitted_timeoffs.config import *
instance = 'trial'

region = 'eu-central-1'
environment = 'pre-production'

company_key = 'necauafmig'
replicon_conn_id = 'NECAUafmig_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'necau_timeoff_delete_can_run_batch_task_{instance}'
disabled = True
