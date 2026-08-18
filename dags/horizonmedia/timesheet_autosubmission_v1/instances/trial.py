# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.timesheet_autosubmission_v1.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'horizonmediatrial01'
replicon_conn_id = 'horizonmediatrial01_replicon_admin'
max_active_runs = 10
execution_timeout_days = 1
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
master_dag_schedule_interval = "0 16 * * FRI"
time_zone = 'America/New_York'
can_run_batch_task_var_name = ""

master_dag_id = f'horizonmedia_timesheet_autosubmission_{instance}_v1'
submit_timesheet_dag_id = f"horizonmedia_timesheet_autosubmission_submit_timesheets_child_dag_{instance}_v1"

base_report_name = "Report_For_Autotimesheet submission_v1"
