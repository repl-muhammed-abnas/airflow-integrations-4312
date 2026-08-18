# pylint: disable=wildcard-import unused-wildcard-import
from pimco.task_status_update_consultant.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'PIMCO'
replicon_conn_id = 'pimco_replicon_admin'

max_active_runs_webhook = 20
max_active_runs_child = 5

#pylint: disable=line-too-long
tenant_email = 'james.stone@pimco.com,david.edwards@pimco.com,alexandria.rausch@pimco.com,scott.schwarmann@pimco.com,shekhar.gupta@pimco.com,mayank.sharma@pimco.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_structure_update = f'pimco_task_structure_update_for_status_can_run_batch_task_{instance}'
can_run_batch_task_master = f'pimco_task_status_update_on_all_active_projects_can_run_batch_task_{instance}'
can_run_batch_task_child = f'pimco_task_status_update_child_can_run_batch_task_{instance}'
can_run_batch_task_webhook = f'pimco_task_status_update_webhook_can_run_batch_task_{instance}'

child_dag_id = f'pimco_consultant_task_status_update_consultant_child_{instance}'
