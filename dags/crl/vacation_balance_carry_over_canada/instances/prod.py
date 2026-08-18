# pylint: disable=wildcard-import unused-wildcard-import
from crl.vacation_balance_carry_over_canada.config import *

instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"

replicon_conn_id = "CharlesRiverLaboratories_repliconint_timeexport"

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,MTL-Payroll@crl.com,Shari.Guttman@crl.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f"crl_annual_vacation_balance_transfer_canada_master_{instance}"
child_dagid = f"crl_annual_vacation_balance_transfer_canada_child_{instance}"

can_run_batch_task_var_name = f"crl_annual_vacation_balance_transfer_run_batch_task_{instance}"
