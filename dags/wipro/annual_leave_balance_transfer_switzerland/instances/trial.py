# pylint: disable=wildcard-import unused-wildcard-import
from wipro.annual_leave_balance_transfer_switzerland.config import *

instance = "trial"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_repliconint"

annual_leaves_balance_transfer_year_end_dag_id = f'wipro_switzerland_annual_leave_balance_transfer_master_{instance}'
child_workflow_to_transfer_timeoff_balance_dag_id = f'wipro_switzerland_annual_leave_balance_transfer_child_{instance}'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"wipro_switzerland_annual_leave_balance_transfer_{instance}_can_run_batch_task"

disabled=True
