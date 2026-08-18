# pylint: disable=wildcard-import unused-wildcard-import
from report_comparison.jobs_data.config import *

instance = "trial"
sftp_conn_id = "sftp_useast2"

# Trial SFTP paths
logs_filepath = "/Trial/ProjectSync/Logs/"

tenant_email = "gayatribadiger@deltek.com"
internal_logs_email = "gayatribadiger@deltek.com"
alerts_email = "gayatribadiger@deltek.com"
workbook_api = "/api/json/reply/DataboardDataRequest/"
workbook_token_var = f"workbook_token_var_{instance}"
workato_token_var = f"workato_token_var_{instance}"
maconomy_api = "maconomy-api/containers/vccpd02/jobs/filter"
maconomy_specification6_api = "maconomy-api/containers/vccpd02/Specification6/filter"

workato_employee_department_api = "/api/lookup_tables/26946/rows?page=1&per_page=5000"
workato_dimensionfeetype_api = "/api/lookup_tables/31430/rows?page=1&per_page=5000"
workato_dimension_income_risk_api = "/api/lookup_tables/31427/rows?page=1&per_page=5000"
workato_dimension_business_unit_api = "/api/lookup_tables/31428/rows?page=1&per_page=5000"
workato_vccp_company_api = "/api/lookup_tables/26942/rows?page=1&per_page=5000"

master_dag_id = f"maconomy_workbook_jobs_report_comparison_{instance}"
replicon_conn_id = "report_comparison_airflow_admin"    
company_key = "Repliconpincstream6dev"


