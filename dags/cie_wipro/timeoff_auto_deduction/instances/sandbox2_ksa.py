# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from cie_wipro.timeoff_auto_deduction.config import *

# for trial , kept it empty to retain the old dag id with logs in QA Env
instance = 'sandbox'
region = 'eu-central-1'
environment = 'pre-production'
company_key = 'wiprosandbox2'
replicon_conn_id = 'wiprosandbox2'
debug = False

tenant_email = "ashishtiwari@deltek.com"
internal_email = "ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_max_active_tasks = 1

instance_tz = "America/New_York"

report_name = "***BaseReport_HoursEntered"
country = "Saudi"
schedule_interval = "0 1 * * *"

max_child_run = 1
execution_timeout_days = 1

date_format = '%d.%m.%Y'
seperator = ";"

time_off_name1 = "KSA - Annual Leave"
time_off_name2 = ""
time_off_name3 = "KSA - Leave Without Pay (LWOP)"
booking_day = 10
minimum_efforts = 4

send_failure_alerts = True
# chat_webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAOTCg1pw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=OZFNzRBbobi0M3nbkmRiit_RSsD0ywCPyGCSeJDBh8E"
chat_webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAc1SZ1YM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=AUPD3BdXsfWBzFDo6sb8hB7PfiUGGqx8chPDLcJbQlg"

can_run_batch_task_var_name = "true"
