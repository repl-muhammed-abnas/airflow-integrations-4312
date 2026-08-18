# pylint: disable=wildcard-import unused-wildcard-import
from wipro.annual_leave_balance_transfer_france.config import *

instance = "uat"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_repliconint"

sftp_conn_id = "sftp_integration_useast"

master_dag = f'wipro_france_annual_leave_balance_transfer_master_{instance}'
child_dag = f'wipro_france_annual_leave_balance_transfer_child_{instance}'

tenant_email = 'france.hrss@wipro.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"wipro_france_annual_leave_balance_transfer_can_run_batch_task_{instance}"
can_force_run = f"wipro_france_annual_leave_balance_transfer_can_force_run_{instance}"
