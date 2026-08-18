# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.timesheet_autosubmission.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'HorizonMediaGen3afmig'
replicon_conn_id = 'horizonmediagen3trial'
max_active_runs = 10
execution_timeout_days = 1
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
master_dag_schedule_interval = "0 16 * * FRI"
time_zone = 'America/New_York'
can_run_batch_task_var_name = ""

disabled=True
