# pylint: disable=wildcard-import unused-wildcard-import
from cefloydcompany.payroll_export.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'CEFloydCompanyafmig'

replicon_conn_id = 'CEFloydCompany_replicon_admin'
sftp_conn_id = "sftp_useast2"
adpexport = '/adpexport/Logs/'


can_run_batch_task_var_name = f'cefloydcompany_adpexport_{instance}_can_run_batch_task'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

disabled=True
