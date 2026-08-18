# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.time_off_recal.config import *

region = 'eu-central-1'
instance = "trial_weekly"
environment = 'pre-production'
company_key = 'pwcfrafmig'

replicon_conn_id = 'pwcfrafmig_replicon_administrator'

log_file_path = 'pwcfrafmig/timeoffrecal/pwc_weekly_timeoffrecal_logs'
new_file_path = 'pwcfrafmig/timeoffrecal/timesheetreapprove_weekly_logs'

schedule_interval = "0 20 * * 6"
time_zone = "Europe/Paris"

upperlimit = 12
lowerlimtmonthwhenfirstworkingday = 2
lowerlimtmonthwhennotfirstworkingday = 2

report_name = 'Timeoff Recal Automation'

can_run_batch_task_child = f'time_off_recal_{instance}_can_run_batch_task'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
