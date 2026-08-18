# pylint: disable=wildcard-import unused-wildcard-import
from crl.user_import_mauritius.config import *
from crl.user_import_mauritius.mappers.user_import_mapper import user_import_mapper
from crl.user_import_mauritius.mappers.employee_type_mapper import employee_type_mapper
from crl.user_import_mauritius.mappers.timeoff_mapper import timeoff_type_mapper


instance = "sandbox"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriesSandbox"
replicon_conn_id = "charlesriverlaboratoriessandbox_repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratoriessandbox_603355"

log_filepath = "/Test/Inbound/User Interface/Logs"
payload_filepath = "/Test/Inbound/User Interface/Archive"

# pylint: disable=line-too-long
tenant_email = "Sean.Cotto@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,Joan.Papandrea@crl.com,Madhuchanda.Choudhury@crl.com,Sacha.Audibert@crl.com,Naveen.Kopparaju@crl.com,Shane.Ureellanah@crl.com,Christophe.Lamusse@crl.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

process_user_import_payload_dagid = f"crl_user_import_mauritius_process_each_payload_child_{instance}"
process_users_dagid = f"crl_user_import_mauritius_process_users_child_{instance}"
process_supervisor_dagid = f"crl_user_import_mauritius_process_pending_supervisor_child_{instance}"
process_log_generation_dagid = f"crl_user_import_mauritius_process_log_generation_child_{instance}"

process_groups_dagid = f"crl_user_import_mauritius_process_groups_child_{instance}"
process_new_company_code_dagid = f"crl_user_import_mauritius_process_new_company_code_child_{instance}"
process_new_locations_dagid = f"crl_user_import_mauritius_process_new_location_child_{instance}"
process_new_buisness_unit_dagid = f"crl_user_import_mauritius_process_new_buisness_unit_child_{instance}"
process_new_cost_center_dagid = f"crl_user_import_mauritius_process_new_cost_center_child_{instance}"
process_new_department_dagid = f"crl_user_import_mauritius_process_new_department_child_{instance}"

process_new_users_dagid = f"crl_user_import_mauritius_process_new_users_child_{instance}"
process_update_users_dagid = f"crl_user_import_mauritius_process_update_users_child_{instance}"
process_disable_users_dagid = f"crl_user_import_mauritius_process_disable_users_child_{instance}"

process_timeoff_type_no_accrual_dagid = f"crl_user_import_mauritius_process_timeoff_type_no_accrual_child_{instance}"
process_timeoff_type_assignment_new_user_dagid = f"crl_user_import_mauritius_process_timeoff_type_new_user_child_{instance}"
process_timeoff_type_assignment_update_rehire_user_dagid = f"crl_user_import_mauritius_process_timeoff_type_update_rehire_user_child_{instance}"

crl_user_import_bearer_token_var = f"crl_user_import_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_mauritius_run_batch_task_{instance}'

USER_MAPPER = user_import_mapper
EMPLOYEE_TYPE_MAPPER = employee_type_mapper
TIMEOFF_TYPE_MAPPER = timeoff_type_mapper

INTEGRATION_USERNAME = 'integration_userimport, Replicon'

disabled=True
