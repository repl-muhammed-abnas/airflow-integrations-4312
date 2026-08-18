# pylint: disable=wildcard-import unused-wildcard-import
from report_comparison.customers.config import *

instance = "production"
sftp_conn_id = "sftp_useast2"
environment = "production"

tenant_email = "BrianBoejden@deltek.com,BaibeColes@deltek.com,jonathan.eyles@vccp.com"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"
alerts_email = "{{ var.value.dagrun_internal_testing_email }}"

workbook_api = "/api/json/reply/DataboardDataRequest/"
workbook_token_var = f"workbook_token_var_{instance}"
workato_token_var = f"workato_token_var_{instance}"
maconomy_api = "maconomy-api/containers/vccp/customercard/filter"

workato_mac_wb_business_unit_api = "/api/lookup_tables/125811/rows?page=1&per_page=6000"
workato_mac_wb_paymentterms_api = "/api/lookup_tables/125825/rows?page=1&per_page=6000"
workato_currency_api = "/api/lookup_tables/125805/rows?page=1&per_page=6000"
workato_mac_wb_industry_api = "/api/lookup_tables/125820/rows?page=1&per_page=6000"

master_dag_id = f"maconomy_workbook_customer_report_comparison_{instance}"
replicon_conn_id = "report_comparison_airflow_admin"
company_key = "Repliconpincstream6uat"
