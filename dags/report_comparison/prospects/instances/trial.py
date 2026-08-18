# pylint: disable=wildcard-import unused-wildcard-import
from report_comparison.prospects.config import *

instance = "trial"
sftp_conn_id = "sftp_useast2"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alerts_email = "{{ var.value.dagrun_internal_testing_email }}"
workbook_api = "/api/json/reply/DataboardDataRequest"
workbook_token_var = f"workbook_token_var_{instance}"
workato_token_var = f"workato_token_var_{instance}"
maconomy_api = "maconomy-api/containers/vccpd02/customercard/filter"
workato_user_interface_name_api = "api/lookup_tables/31446/rows?page=1&per_page=5000"

master_dag_id = f"maconomy_workbook_prospect_report_comparison_{instance}"
replicon_conn_id = "report_comparison_airflow_admin"
company_key = "Repliconpincstream6dev"

disable=True