# pylint: disable=wildcard-import unused-wildcard-import
from report_comparison.customers.config import *

instance = "trial"
sftp_conn_id = "sftp_useast2"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alerts_email = "{{ var.value.dagrun_internal_testing_email }}"
workbook_api = "/api/json/reply/DataboardDataRequest/"
workbook_token_var = f"workbook_token_var_{instance}"
workato_token_var = f"workato_token_var_{instance}"
maconomy_api = "maconomy-api/containers/vccpd02/customercard/filter"
# workato_employee_department_api = "/api/lookup_tables/26946/rows?page=1&per_page=5000"

workato_mac_wb_business_unit_api = "/api/lookup_tables/31428/rows?page=1&per_page=6000"
workato_mac_wb_paymentterms_api = "/api/lookup_tables/31329/rows?page=1&per_page=6000"
workato_currency_api = "/api/lookup_tables/28646/rows?page=1&per_page=6000"
workato_mac_wb_industry_api = "/api/lookup_tables/31446/rows?page=1&per_page=6000"

master_dag_id = f"maconomy_workbook_customer_report_comparison_{instance}"
replicon_conn_id = "report_comparison_airflow_admin"
company_key = "Repliconpincstream6dev"

disabled = True

