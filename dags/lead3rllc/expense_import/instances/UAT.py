from lead3rllc.expense_import.config import *

instance = 'uat'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'lead3rllctrial01'
replicon_conn_id = 'lead3rllctrial01_Integration.user'

user_loginname_for_report = 'Expenses'
user_loginname_for_invoice = 'Invoice'

sftp_conn_id = 'sftp_lead3rllc_696576'

sftp_input_filepath_report = '/UAT/Expense Import/Expense Reports/Input'
sftp_archive_filepath_report = '/UAT/Expense Import/Expense Reports/Archive'

sftp_input_filepath_invoice = '/UAT/Expense Import/Invoices/Input'
sftp_archive_filepath_invoice = '/UAT/Expense Import/Invoices/Archive'

tenant_email = "lydia.tuch@lead3r.com,Billing@lead3r.com,accounting@lead3r.com,ena.park@huddl3.group,lucas.reichart@huddl3.group,Joshua.rogers@huddl3.group"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

expense_report_master_dag_id = f'lead3rllc_expense_import_report_master_{instance}'

child_create_expense_sheet_for_report_dag_id = f'lead3rllc_expense_import_report_create_expense_sheet_child_{instance}'
process_log_generation_for_report_dag_id = f'lead3rllc_expense_import_report_process_log_generation_child_{instance}'

expense_invoice_master_dag_id = f'lead3rllc_expense_import_invoice_master_{instance}'

child_create_expense_sheet_for_invoice_dag_id = f'lead3rllc_expense_import_invoice_create_expense_sheet_child_{instance}'
process_log_generation_for_invoice_dag_id = f'lead3rllc_expense_import_invoice_process_log_generation_child_{instance}'

can_run_batch_task_var_name = f'lead3rllc_expense_import_batch_task_var_{instance}'
