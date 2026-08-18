# pylint: disable=wildcard-import unused-wildcard-import
from crjg3.payroll_export.config import *

instance = "prod"
environment = 'production'
company_key = 'CRJG3'

bcc_tenant_email = '{{ var.value.dagrun_internal_log_email }}'

replicon_conn_id = 'crjg3_replicon_admin'
bearer_token_var = f'crjg3_webhooks_{instance}_secret'

can_run_batch_task_var_name = f'crjg3_payroll_export_can_run_batch_task{instance}'
