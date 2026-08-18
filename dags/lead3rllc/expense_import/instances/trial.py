from lead3rllc.expense_import.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'lead3rllctrial01'
replicon_conn_id = 'lead3rllctrial01_Integration.user'

user_loginname_for_report = 'Expenses'
user_loginname_for_invoice = 'Invoice'

sftp_conn_id = 'rsftp_afmig_useast2'

sftp_input_filepath_report = 'lead3rllc/expense_import/report/input'
sftp_archive_filepath_report = 'lead3rllc/expense_import/report/archive'

sftp_input_filepath_invoice = 'lead3rllc/expense_import/invoice/input'
sftp_archive_filepath_invoice = 'lead3rllc/expense_import/invoice/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

expense_report_master_dag_id = f'lead3rllc_expense_import_report_master_{instance}'

child_create_expense_sheet_for_report_dag_id = f'lead3rllc_expense_import_report_create_expense_sheet_child_{instance}'
process_log_generation_for_report_dag_id = f'lead3rllc_expense_import_report_process_log_generation_child_{instance}'

expense_invoice_master_dag_id = f'lead3rllc_expense_import_invoice_master_{instance}'

child_create_expense_sheet_for_invoice_dag_id = f'lead3rllc_expense_import_invoice_create_expense_sheet_child_{instance}'
process_log_generation_for_invoice_dag_id = f'lead3rllc_expense_import_invoice_process_log_generation_child_{instance}'

can_run_batch_task_var_name = f'lead3rllc_expense_import_batch_task_var_{instance}'

disabled=True
