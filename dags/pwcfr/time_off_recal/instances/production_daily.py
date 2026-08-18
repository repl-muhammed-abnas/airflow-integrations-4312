# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.time_off_recal.config import *

region = 'eu-central-1'
instance = "production_daily"
environment = 'production'
company_key = 'pwcfr'

replicon_conn_id = 'pwcfr_replicon_admin'

log_file_path = 'PWCFR/timeoffrecal/timeoffrecal_logs'
new_file_path = 'PWCFR/timeoffrecal/timesheetreapprove_logs'

schedule_interval = "0 7 * * *"
time_zone = "Europe/Paris"

upperlimit = 0
lowerlimtmonthwhenfirstworkingday = 2
lowerlimtmonthwhennotfirstworkingday = 1

report_name = 'Timeoff Recal Automation'

can_run_batch_task_child = f'time_off_recal_{instance}_can_run_batch_task'

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
