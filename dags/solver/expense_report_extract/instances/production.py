# pylint: disable=wildcard-import unused-wildcard-import
from solver.expense_report_extract.config import *

environment = 'production'
instance = 'production'

company_key = 'Solver'

replicon_conn_id = 'solver_replicon_sgamber@solverglobal.com'

tenant_email = 'expenses@solverglobal.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'solver_expense_report_extract_can_run_batch_task_{instance}'
