# pylint: disable=wildcard-import unused-wildcard-import
from daimlertrucks.custom_email_notification.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'DaimlerTrucksafmig'
replicon_conn_id = 'DaimlerTrucksafmig'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
execution_timeout_days = 1
can_run_batch_task_var_name = f"daimlertrucks_custom_email_notification_{instance}_can_run_batch_task"
master_dag_schedule_interval = "0 18 * * Mon-Fri"
disabled = True
