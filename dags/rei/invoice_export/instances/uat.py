# pylint: disable=wildcard-import unused-wildcard-import
from rei.invoice_export.config import *

instance = "uat"
environment = 'production'
company_key = 'reitrial01'

replicon_conn_id = f'standard_qbo_{company_key}_replicon'
sftp_conn_id = "sftp_rei_672042"
tenant_email = 'steven.murff@reiutilityservices.com,kylie.friedrich@reiutilityservices.com'

report_file_path="/UAT/Timesheet_Report"

can_run_batch_task_var_name = f'{company_key}_quickbooks_online_invoice_export_{instance}_can_run_batch_task'
