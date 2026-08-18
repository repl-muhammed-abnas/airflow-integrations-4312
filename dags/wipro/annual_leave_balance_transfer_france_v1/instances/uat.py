# pylint: disable=wildcard-import unused-wildcard-import
from wipro.annual_leave_balance_transfer_france_v1.config import *

instance = "uat"

region = 'eu-central-1'
environment = "pre-production"

company_key = "Wiprosandbox2"
replicon_conn_id = "wiprosandbox2_repliconint"
sftp_conn_id = "sftp_integration_useast"

version = 'v1'

master_dag = f'wipro_france_annual_leave_balance_transfer_master_{instance}_{version}'
child_dag = f'wipro_france_annual_leave_balance_transfer_child_{instance}_{version}'
whit_monday_master_dag = f'wipro_france_whit_monday_deduction_master_{instance}'

tenant_email = 'france.hrss@wipro.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"wipro_france_annual_leave_balance_transfer_can_run_batch_task_{instance}_{version}"
can_force_run = f"wipro_france_annual_leave_balance_transfer_can_force_run_{instance}_{version}"
