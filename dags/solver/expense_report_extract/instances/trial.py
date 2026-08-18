# pylint: disable=wildcard-import unused-wildcard-import
from solver.expense_report_extract.config import *

environment = 'pre-production'
instance = 'trial'

replicon_conn_id = 'solver_replicon_sgamber@solverglobal.com'
sftp_conn_id = 'sftp_useast2'

upload_filepath = 'solver/'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'solver_expense_report_extract_can_run_batch_task_{instance}'
