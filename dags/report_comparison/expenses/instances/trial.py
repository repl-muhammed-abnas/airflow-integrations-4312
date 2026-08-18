from report_comparison.expenses.config import *

instance = "trial"
sftp_conn_id = "sftp_useast2"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alerts_email = "{{ var.value.dagrun_internal_testing_email }}"
workbook_api = "/api/json/reply/DataboardDataRequest/"
workbook_token_var = f"workbook_token_var_{instance}"
workato_token_var = f"workato_token_var_{instance}"
maconomy_api = "maconomy-api/containers/vccpd02/ExpenseSheetLines/filter"
maconomy_emp_api = "maconomy-api/containers/vccpd02/employees/filter"
workato_purchases_vat = "/api/lookup_tables/31421/rows?page=1&per_page=5000"

master_dag_id = f"maconomy_workbook_expenses_comparison_{instance}"
replicon_conn_id = "repliconinc_replicon_replicon.polaris"
company_key = "Repliconpincstream6dev"