# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.timesheet_autosubmission.config import *

region = 'us-east-1'
instance = "production"
environment = 'production'
company_key = 'Horizonmedia'
replicon_conn_id = 'horizonmedia_repliconadmin_replicon'
max_active_runs = 10
execution_timeout_days = 1
tenant_email = 'gfraga@horizonmedia.com,Sgrandi@horizonmedia.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
master_dag_schedule_interval = "0 16 * * FRI"
time_zone = 'America/New_York'
can_run_batch_task_var_name = ""
