# pylint: disable=wildcard-import unused-wildcard-import
from crl.user_import_generic_row.countries.israel.country_config import *  # noqa: F401,F403
from crl.user_import_generic_row.countries.israel.mappers.user_import_mapper import user_import_mapper


region = "us-east-1"
instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"
replicon_conn_id = "CharlesRiverLaboratories_replicon_Repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratories_603355"

log_filepath = "/Production/Inbound/User Interface/Logs"
payload_filepath = "/Production/Inbound/User Interface/Archive"

# pylint: disable=line-too-long
tenant_email = 'Bal-hr@crl.com,Valerie.McGrath@crl.com,SAPCPISUPPORT@charlesriverlabs.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,Prabhav.Potluri@crl.com,Sean.Cotto@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
version = ""
dag_postfix = f'{instance}{version}'

process_user_import_payload_dagid = f"{DAG_PREFIX}_process_each_payload_child_{dag_postfix}"
process_users_dagid = f"{DAG_PREFIX}_process_users_child_{dag_postfix}"
process_supervisor_dagid = f"{DAG_PREFIX}_process_pending_supervisor_child_{dag_postfix}"
process_log_generation_dagid = f"{DAG_PREFIX}_process_log_generation_child_{dag_postfix}"

process_groups_dagid = f"{DAG_PREFIX}_process_groups_child_{dag_postfix}"
process_new_company_code_dagid = f"{DAG_PREFIX}_process_new_company_code_child_{dag_postfix}"
process_new_locations_dagid = f"{DAG_PREFIX}_process_new_location_child_{dag_postfix}"
process_new_buisness_unit_dagid = f"{DAG_PREFIX}_process_new_buisness_unit_child_{dag_postfix}"
process_new_cost_center_dagid = f"{DAG_PREFIX}_process_new_cost_center_child_{dag_postfix}"
process_new_department_dagid = f"{DAG_PREFIX}_process_new_department_child_{dag_postfix}"

process_new_users_dagid = f"{DAG_PREFIX}_process_new_users_child_{dag_postfix}"
process_update_users_dagid = f"{DAG_PREFIX}_process_update_users_child_{dag_postfix}"
process_disable_users_dagid = f"{DAG_PREFIX}_process_disable_users_child_{dag_postfix}"

process_timeoff_type_no_accrual_dagid = f"{DAG_PREFIX}_process_timeoff_type_no_accrual_child_{dag_postfix}"
process_timeoff_type_assignment_new_user_dagid = f"{DAG_PREFIX}_process_timeoff_type_new_user_child_{dag_postfix}"
process_timeoff_type_assignment_update_rehire_user_dagid = f"{DAG_PREFIX}_process_timeoff_type_update_rehire_user_child_{dag_postfix}"

crl_user_import_bearer_token_var = f"crl_user_import_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_run_batch_task_{instance}'

USER_MAPPER = user_import_mapper

INTEGRATION_USERNAME = 'integration_userimport, Replicon'
