# pylint: disable=wildcard-import unused-wildcard-import
from cie_randstadlifescience.expenseDataExport.config import *

instance = 'trial'  # for trial , kept it empty to retain the old dag id with logs in QA Env
region = 'us-east-1'
environment = 'pre-production'
company_key = 'Randstadtrial01'
replicon_conn_id = 'Randstadtrial01'
debug = False

# for webhook
bucket_name = "replicon-integrations-uswest"  # "replicon-airflow-dev-cie-group"
file_path = "Randstadtrial01"
file_name = "ExpenseSheetStatusChangedToApproved_{}.csv"
team_id = "CIE"

sftp_conn_id = 'randstadtrialSFTP'

tenant_email = "ashishtiwari@deltek.com"
internal_email = "ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_max_active_tasks = 128
user_base_report_name = "CustomUserDetails_1.0"
projectdetails_base_report_name = "CustomProjectDetails_1.0"
chunk_size = 300
based_report_date_format_with_time = "%b %d, %Y %I:%M:%S %p"
export_date_time_format = "%m/%d/%Y %I:%M:%S.000 %p"

sftp_filepath = "/Trial/Expense Data Extract/"
export_filename = "RPL_Solutions_Expense_"
output_export_file_timestamp_format = "%Y%m%d%H%M%S"

expense_detail_file_path = "Randstadtrial01"
expense_detail_bucket = "replicon-integrations-uswest"

processed_expense_uris_file_path = "artifacts/Randstadtrial01/ProcessedExpenseUris"
processed_expense_uris_file_name = "expense_uris.txt"
processed_expense_uris_bucket_name = "replicon-airflow-dev-cie-group"

schedule_interval = "30,00 7,13 * * TUE,WED,THU,FRI"
instance_tz = "America/New_York"

trigger_time = [
    "Tuesday 7:00", "Tuesday 13:00", "Wednesday 7:30", "Thursday 7:30", "Friday 7:30"
]

disable = True

disabled = True
