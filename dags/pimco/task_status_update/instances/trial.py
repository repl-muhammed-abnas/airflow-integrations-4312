# pylint: disable=wildcard-import unused-wildcard-import
from pimco.task_status_update.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'pimcoafmig'
replicon_conn_id = 'pimcoafmig_replicon_admin'

max_active_runs_webhook = 20
max_active_runs_child = 5

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_structure_update = f'pimco_task_structure_update_for_status_{instance}_can_run_batch_task'
can_run_batch_task_master = f'pimco_task_status_update_on_all_active_projects_{instance}_can_run_batch_task'
can_run_batch_task_child = f'pimco_task_status_update_child_{instance}_can_run_batch_task'
can_run_batch_task_webhook = f'pimco_task_status_update_webhook_{instance}_can_run_batch_task'

child_dag_id = f'pimco_task_status_update_child_{instance}'
disabled = True
