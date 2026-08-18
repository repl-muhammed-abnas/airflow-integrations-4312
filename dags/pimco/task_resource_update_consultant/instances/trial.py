# pylint: disable=wildcard-import unused-wildcard-import
from pimco.task_resource_update_consultant.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'PIMCOTrial02'
replicon_conn_id = 'pimcotrial02-admin'

max_active_runs_webhook = 20
max_active_runs_child = 5

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
can_run_batch_task_child = f'pimco_resource_assignment_update_to_task_child_{instance}_can_run_batch_task'
can_run_batch_task_master = f'pimco_task_resource_update_consultant_on_all_active_projects_master_{instance}_can_run_batch_task'
can_run_batch_task_webhook = f'pimco_task_resource_update_consultant_webook_{instance}_can_run_batch_task'

child_dag_id = f'pimco_resource_assignment_update_to_consultant_task_child_{instance}'
disabled = True
