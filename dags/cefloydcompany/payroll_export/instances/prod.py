# pylint: disable=wildcard-import unused-wildcard-import
from cefloydcompany.payroll_export.config import *

region = 'us-east-1'
instance = "prod"
environment = 'production'
company_key = 'CEFloydCompany'

replicon_conn_id = 'CEFloydCompany_replicon_admin'

can_run_batch_task_var_name = f'cefloydcompany_adpexport_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
