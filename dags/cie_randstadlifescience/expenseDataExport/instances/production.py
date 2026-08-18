# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from cie_randstadlifescience.expenseDataExport.config import *

# for trial , kept it empty to retain the old dag id with logs in QA Env
instance = 'production'
region = 'us-east-1'
environment = 'production'
company_key = 'Randstad'
replicon_conn_id = 'Randstad'
debug = False

# for webhook
bucket_name = "replicon-integrations-uswest"  # "replicon-airflow-dev-cie-group"
file_path = "Randstad"
file_name = "ExpenseSheetStatusChangedToApproved_{}.csv"
team_id = "CIE"

sftp_conn_id = 'RandstadProductionSFTP'

tenant_email = 'rgs-noc-aix-dl@randstadusa.com,RUS-PayTime.Agileteam-DL@randstadusa.com'
internal_email = "PIEReplicon@deltek.com,ashishtiwari@deltek.com,AnjaliMer@deltek.com,KatieZuccarelli@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_max_active_tasks = 128
user_base_report_name = "CustomUserDetails_1.0"
projectdetails_base_report_name = "CustomProjectDetails_1.0"
chunk_size = 300
based_report_date_format_with_time = "%b %d, %Y %I:%M:%S %p"
export_date_time_format = "%m/%d/%Y %I:%M:%S.000 %p"

sftp_filepath = "prod/toRandstad/"
export_filename = "RPL_Solutions_Expense_"
output_export_file_timestamp_format = "%Y%m%d%H%M%S"

# same as webhook
expense_detail_file_path = "Randstad"
expense_detail_bucket = "replicon-integrations-uswest"

processed_expense_uris_file_path = "artifacts/Randstad/ProcessedExpenseUris"
processed_expense_uris_file_name = "expense_uris.txt"
processed_expense_uris_bucket_name = "replicon-airflow-dev-cie-group"

schedule_interval = "30 6,7 * * WED,THU,FRI"
instance_tz = "America/New_York"

trigger_time = [
    "Wednesday 6:30", "Wednesday 7:30", "Thursday 6:30", "Thursday 7:30", "Friday 6:30", "Friday 7:30"
]
