# pylint: disable=wildcard-import unused-wildcard-import
from wipro.annual_leave_balance_transfer_netherlands_v1.config import *

instance = "trial"
version = "v1"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

sftp_conn_id = "sftp_useast2"

log_filepath = '/wipro/annual_leave_timeoff_balance_transfer/Logs'

annual_leaves_to_carried_over_dag_id = f'wipro_netherlands_annual_leave_balance_transfer_year_end_{instance}_{version}'
annual_leaves_carried_over_to_lapsed_dag_id = f'wipro_netherlands_annual_leave_balance_transfer_carried_over_{instance}_{version}'
annual_leaves_carried_over_to_lapsed_probation_users_dag_id = f'wipro_netherlands_annual_leave_balance_transfer_carried_over_probation_users_{instance}_{version}'
child_workflow_to_transfer_timeoff_balance_dag_id = f'wipro_netherlands_annual_leave_balance_transfer_workflow_to_tranfer_balance_{instance}_{version}'

tenant_email = 'sindhuja.r14@wipro.com,loredana.barsan@wipro.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"wipro_netherlands_annual_leave_balance_transfer_{instance}_{version}_can_run_batch_task"
