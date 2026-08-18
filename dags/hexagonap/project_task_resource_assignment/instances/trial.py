# pylint: disable=wildcard-import unused-wildcard-import
from hexagonap.project_task_resource_assignment.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'hexagonapafmig'
replicon_conn_id = 'hexagonapafmig_replicon_admin'
time_zone = 'America/Chicago'
schedule_interval = '0 9,15,21 * * *'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'


can_run_batch_task = f'hexagonap_project_task_resource_assignment_can_run_batch_task_{instance}'

disabled=True
