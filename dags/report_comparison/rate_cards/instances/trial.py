# pylint: disable=wildcard-import unused-wildcard-import
from report_comparison.rate_cards.config import *

instance = "trial"
sftp_conn_id = "sftp_useast2"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alerts_email = "{{ var.value.dagrun_internal_testing_email }}"
workbook_api = "/api/json/reply/DataboardDataRequest"
workbook_token_var = f"workbook_token_var_{instance}"
workato_token_var = f"workato_token_var_{instance}"
maconomy_api = "maconomy-api/containers/vccpd02/jobpricelists/filter"

master_dag_id = f"maconomy_workbook_rate_cards_report_comparison_{instance}"
replicon_conn_id = "report_comparison_airflow_admin"
company_key = "Repliconpincstream6dev"
disable = True