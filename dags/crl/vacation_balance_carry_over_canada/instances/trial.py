# pylint: disable=wildcard-import unused-wildcard-import
from crl.vacation_balance_carry_over_canada.config import *

instance = "trial"

company_key = "CharlesRiverLaboratoriestrial01"

replicon_conn_id = "charlesriverlaboratoriestrial01_replicon_repliconadmin"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dagid = f"crl_annual_vacation_balance_transfer_canada_master_{instance}"
child_dagid = f"crl_annual_vacation_balance_transfer_canada_child_{instance}"

can_run_batch_task_var_name = f"crl_annual_vacation_balance_transfer_run_batch_task_{instance}"

disabled=True
