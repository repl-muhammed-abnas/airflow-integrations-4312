# pylint: disable=wildcard-import unused-wildcard-import
from rei.invoice_export.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'reiafmig'

replicon_conn_id = 'standard_qbo_reiafmig_replicon'
sftp_conn_id = "sftp_internal"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

report_file_path="/shivam/qbo/rei/report"

can_run_batch_task_var_name = f'{company_key}_quickbooks_online_invoice_export_{instance}_can_run_batch_task'
