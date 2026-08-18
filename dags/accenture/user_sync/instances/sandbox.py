# pylint: disable=wildcard-import unused-wildcard-import
from accenture.user_sync.config import *

instance = 'sandbox'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'Accenturesandbox'

vantagepoint_conn_id = 'accenturesandbox_vantagepoint_tus'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'accenture_user_sync_can_run_batch_task_{instance}'

employee_sync_dag_id = f'accenture_user_sync_mrdr_master_{instance}'
process_employee_child_dag_id = f'accenture_user_sync_mrdr_child_{instance}'
