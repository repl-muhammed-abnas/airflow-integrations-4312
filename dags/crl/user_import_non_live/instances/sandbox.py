# pylint: disable=wildcard-import unused-wildcard-import
from crl.user_import_non_live.config import *

instance = "sandbox"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriesSandbox"
replicon_conn_id = "charlesriverlaboratoriessandbox_repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratoriessandbox_603355"

log_filepath = "/Test/Inbound/User Interface/Logs"
payload_filepath = "/Test/Inbound/User Interface/Archive"

# pylint: disable=line-too-long
tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,MTL-Payroll@crl.com,Shari.Guttman@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


process_user_import_payload_dagid = f"crl_user_import_others_process_each_payload_child_{instance}"
process_users_dagid = f"crl_user_import_others_process_users_child_{instance}"
process_log_generation_dagid = f"crl_user_import_others_process_log_generation_child_{instance}"
process_new_users_dagid = f"crl_user_import_others_process_new_users_child_{instance}"
process_update_users_dagid = f"crl_user_import_others_process_update_users_child_{instance}"

process_timeoff_type_no_accrual_dagid = f"crl_user_import_others_process_timeoff_type_no_accrual_child_{instance}"

crl_user_import_bearer_token_var = f"crl_user_import_others_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_others_run_batch_task_{instance}'
