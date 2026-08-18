# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from cie_wipro.KSA_Defaulter_Report.config import *

# for trial , kept it empty to retain the old dag id with logs in QA Env
instance = 'sandbox'
environment = 'pre-production'
company_key = 'wiprosandbox2'
replicon_conn_id = 'wiprosandbox2'
debug = False

tenant_email = "ashishtiwari@deltek.com,hemanthss@deltek.com"
internal_email = "ashishtiwari@deltek.com,hemanthss@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_max_active_tasks = 128

instance_tz = "America/New_York"

report_name = "***BaseReport_TimeoffBooking_DefaulterReport"
country = "Saudi"
schedule_interval = "0 6 5 * *"

time_off_comments_value = "Attendance/Effort Not Marked"
max_child_run = 3
execution_timeout_days = 1

date_format = '%d.%m.%Y'
seperator = ";"

booking_count = 5


send_failure_alerts = True
# chat_webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAOTCg1pw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=OZFNzRBbobi0M3nbkmRiit_RSsD0ywCPyGCSeJDBh8E"
chat_webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAc1SZ1YM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=AUPD3BdXsfWBzFDo6sb8hB7PfiUGGqx8chPDLcJbQlg"

can_run_batch_task_var_name = "true"
