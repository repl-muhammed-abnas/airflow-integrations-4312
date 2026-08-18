# pylint: disable=wildcard-import unused-wildcard-import
from crl.user_import_united_kingdom.config import *
from crl.user_import_united_kingdom.mappers.annual_leave_placeholder_mapper import annual_leave_placeholder_timeoff_types
from crl.user_import_united_kingdom.mappers.sick_leave_placeholder_mapper import sick_time_off_type_placeholder_mapper
from crl.user_import_united_kingdom.mappers.user_import_mapper import user_import_mapper


instance = "sandbox"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriesSandbox"
replicon_conn_id = "charlesriverlaboratoriessandbox_repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratoriessandbox_603355"

log_filepath = "/Test/Inbound/User Interface/Logs"
payload_filepath = "/Test/Inbound/User Interface/Archive"

# pylint: disable=line-too-long
tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,Prabhav.Potluri@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version= "" #_v1,_v2 etc
dag_postfix = f'{instance}{version}'

process_user_import_payload_dagid = f"crl_user_import_uk_process_each_payload_child_{dag_postfix}"
process_users_dagid = f"crl_user_import_uk_process_users_child_{dag_postfix}"
process_supervisor_dagid = f"crl_user_import_uk_process_pending_supervisor_child_{dag_postfix}"
process_log_generation_dagid = f"crl_user_import_uk_process_log_generation_child_{dag_postfix}"

process_groups_dagid = f"crl_user_import_uk_process_groups_child_{dag_postfix}"
process_new_company_code_dagid = f"crl_user_import_uk_process_new_company_code_child_{dag_postfix}"
process_new_locations_dagid = f"crl_user_import_uk_process_new_location_child_{dag_postfix}"
process_new_buisness_unit_dagid = f"crl_user_import_uk_process_new_buisness_unit_child_{dag_postfix}"
process_new_cost_center_dagid = f"crl_user_import_uk_process_new_cost_center_child_{dag_postfix}"
process_new_department_dagid = f"crl_user_import_uk_process_new_department_child_{dag_postfix}"

process_new_users_dagid = f"crl_user_import_uk_process_new_users_child_{dag_postfix}"
process_update_users_dagid = f"crl_user_import_uk_process_update_users_child_{dag_postfix}"
process_disable_users_dagid = f"crl_user_import_uk_process_disable_users_child_{dag_postfix}"

process_timeoff_type_no_accrual_dagid = f"crl_user_import_uk_process_timeoff_type_no_accrual_child_{dag_postfix}"
process_timeoff_type_assignment_new_user_dagid = f"crl_user_import_uk_process_timeoff_type_new_user_child_{dag_postfix}"
process_timeoff_type_assignment_update_rehire_user_dagid = f"crl_user_import_uk_process_timeoff_type_update_rehire_user_child_{dag_postfix}"

crl_user_import_bearer_token_var = f"crl_user_import_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_run_batch_task_{instance}'

USER_MAPPER = user_import_mapper

ANNUAL_TO_PLACEHOLDER = annual_leave_placeholder_timeoff_types
SICK_TO_PLACEHOLDER = sick_time_off_type_placeholder_mapper

INTEGRATION_USERNAME = 'integration_userimport, Replicon'
