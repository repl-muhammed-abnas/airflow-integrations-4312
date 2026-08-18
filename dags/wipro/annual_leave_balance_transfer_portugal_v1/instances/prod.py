# pylint: disable=wildcard-import unused-wildcard-import
from wipro.annual_leave_balance_transfer_portugal_v1.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_repliconint"

sftp_conn_id = "sftp_useast2_internal"

log_filepath = '/wipro/annual_leave_balance_transfer_portugal/PROD/Logs'

version = "v1"

annual_leaves_to_carried_over_dag_id = f'wipro_annual_leave_balance_transfer_portugal_year_end_{instance}_{version}'
annual_leaves_carried_over_to_lapsed_dag_id = f'wipro_annual_leave_balance_transfer_portugal_carried_over_{instance}_{version}'
annual_leaves_carried_over_to_lapsed_probation_users_dag_id = f'wipro_annual_leave_balance_transfer_portugal_carried_over_probation_users_{instance}_{version}'
child_workflow_to_transfer_timeoff_balance_dag_id = f'wipro_annual_leave_balance_transfer_portugal_workflow_to_transfer_balance_{instance}_{version}'

tenant_email = 'portugal.hrss@wipro.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task_var_name = f"wipro_annual_leave_balance_transfer_portugal_{instance}_can_run_batch_task"
