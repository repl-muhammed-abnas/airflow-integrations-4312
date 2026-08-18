# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from cie_pwcglobal.toil_extract.config import *

company_key = 'PwC'
environment = 'production'
region = 'eu-central-1'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_email = "PIEReplicon@deltek.com, bartosz.polawski@pwc.com, damian.korpas@pwc.com"
internal_logs_email = "PIEReplicon@deltek.com,ChandanRai@deltek.com, bartosz.polawski@pwc.com, damian.korpas@pwc.com"

schedule_interval = "0 13 * * *"
schedule_timezone = "Europe/Amsterdam"
time_zone = "UTC"

replicon_conn_id = "replicon_PwC"
aws_conn_id = 'replicon.CIE_aws'
# replicon-cie is the AWS account name
s3_bucket_name = 'replicon-airflow-dev-cie-group'

file_extension = '.csv'

toil_extract_file_name = 'TOIL_Hours_Extract_{0}_MLT'
timestamp = '%Y%m%d%H%M%S'
s3_output_filepath = "PwC/Toil_Extract/Output/"

reference_file_name = "Toil_Reference_Malta"
# Please create the Reference File with header [in s3_reference_filepath] for new Deployments. For headers, refer to `dags/cie_pwcglobal/toil_extract/Toil_Reference_{example_location}.csv`
s3_reference_filepath = "PwC/Toil_Extract/Reference/"
s3_reference_archive_filepath = "PwC/Toil_Extract/Reference/Archive/"

sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'
sftp_output_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Outbound/Time/'

location = 'Malta'
dag_id_post_fix = "_".join('Malta'.split()) # location name to display in dag id(without spaces and special characters)
toil_to_types = ['MLT_Time off in lieu(TOIL)']

prev_period_in_months = 3
report_config = {
    "timesheet_day_template_report_name": "***TOIL Extract TS Day Report - Malta",
    "timeoff_transaction_report_name": "***TOIL Extract TO Transaction Report - Malta",
    "user_template_report_name": "***TOIL Extract User Template Report - Malta",
}
report_sep = ","
report_filter_date_format = "%m/%d/%Y"
date_format = '%d/%m/%Y'
output_date_format = '%d/%m/%Y'

# send_fail_msg_to_hangouts = True
# chat_webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAOTCg1pw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=OZFNzRBbobi0M3nbkmRiit_RSsD0ywCPyGCSeJDBh8E"
