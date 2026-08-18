# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from cie_wipro.efforts_notification.config import *

# for trial , kept it empty to retain the old dag id with logs in QA Env
instance = 'sandbox'
environment = 'pre-production'
company_key = 'wiprosandbox1'
replicon_conn_id = 'wiprosandbox1'
debug = False

tenant_email = "ashishtiwari@deltek.com"
internal_email = "ashishtiwari@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_max_active_tasks = 128

instance_tz = "America/New_York"

report_name = "***BaseReport_HoursEntered_Ireland"
country = "ireland"
schedule_interval = "0 1 * * *"

max_child_run = 3
execution_timeout_days = 1

date_format = '%d.%m.%Y'
seperator = ";"

trigger_1 = 6
final_trigger = 8
booking_date = 10

minimum_efforts = 4

hr_login_name = "G121004@Wipro.com"
send_failure_alerts = True

chat_webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAc1SZ1YM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=AUPD3BdXsfWBzFDo6sb8hB7PfiUGGqx8chPDLcJbQlg"

can_run_batch_task_var_name = "true"
