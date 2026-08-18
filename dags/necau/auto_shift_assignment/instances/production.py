# pylint: disable=wildcard-import unused-wildcard-import
from necau.auto_shift_assignment.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'
company_key = 'necau'
replicon_conn_id = 'necau-replicon-admin'
tenant_email = "HRISSupport@nec.com.au"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'necau_auto_shift_run_batch_task_{instance}'
