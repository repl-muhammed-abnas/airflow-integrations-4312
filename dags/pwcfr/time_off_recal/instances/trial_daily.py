# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.time_off_recal.config import *

region = 'eu-central-1'
instance = "trial_daily"
environment = 'pre-production'
company_key = 'pwcfrafmig'

replicon_conn_id = 'pwcfrafmig_replicon_administrator'

log_file_path = 'pwcfrafmig/timeoffrecal/timeoffrecal_logs'
new_file_path = 'pwcfrafmig/timeoffrecal/timesheetreapprove_logs'

schedule_interval = "0 7 * * *"
time_zone = "Europe/Paris"

upperlimit = 0
lowerlimtmonthwhenfirstworkingday = 2
lowerlimtmonthwhennotfirstworkingday = 1

report_name = 'Timeoff Recal Automation'

can_run_batch_task_child = f'time_off_recal_{instance}_can_run_batch_task'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
