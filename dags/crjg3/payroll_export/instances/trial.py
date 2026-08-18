# pylint: disable=wildcard-import unused-wildcard-import
from crjg3.payroll_export.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'crjg3afmig'

bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

replicon_conn_id = 'crjg3afmig_replicon_admin'
bearer_token_var = f'crjg3afmig_webhooks_{instance}_secret'

can_run_batch_task_var_name = f'crjg3_payroll_export_can_run_batch_task{instance}'
