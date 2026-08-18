# pylint: disable=wildcard-import unused-wildcard-import
from wipro.annual_leave_balance_transfer_switzerland.config import *

instance = "prod"

environment = 'production'

company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_repliconint"

annual_leaves_balance_transfer_year_end_dag_id = f'wipro_switzerland_annual_leave_balance_transfer_master_{instance}'
child_workflow_to_transfer_timeoff_balance_dag_id = f'wipro_switzerland_annual_leave_balance_transfer_child_{instance}'

tenant_email = 'Switzerland.hrss@wipro.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task_var_name = f"wipro_switzerland_annual_leave_balance_transfer_{instance}_can_run_batch_task"
