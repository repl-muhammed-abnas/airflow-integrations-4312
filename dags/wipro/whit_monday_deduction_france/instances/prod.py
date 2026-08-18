# pylint: disable=wildcard-import unused-wildcard-import
from wipro.whit_monday_deduction_france.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "WiproLimited"
replicon_conn_id = "wiprolimited_repliconint"

version = ''

master_dag = f'wipro_france_whit_monday_deduction_master_{instance}{version}'
child_dag = f'wipro_france_whit_monday_deduction_child_{instance}{version}'

tenant_email = 'france.hrss@wipro.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_force_run = f"wipro_france_whit_monday_deduction_can_force_run_{instance}{version}"
can_run_batch_task_var_name = f"wipro_france_whit_monday_deduction_can_run_batch_task_{instance}{version}"
