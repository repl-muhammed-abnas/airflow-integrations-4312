# pylint: disable=wildcard-import unused-wildcard-import
from hostopia.jira_integration.config import *

instance = "trial"

region = 'eu-central-1'
environment = 'pre-production'
company_key = 'Hostopiatrial01'

schedule_interval = '0 */1 * * *'
pacific_timezone = 'America/Los_Angeles'
replicon_conn_id = "hostopia-replicon-admin"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
execution_timeout_days = 14
child_dag_process_wbs_max_active_runs = 14
master_dag_max_active_runs = 1
second_master_dag_max_active_runs=6
can_run_batch_task_var_name = f'hostopia_jira_import_{instance}_can_run_batch_task'

disabled = True
