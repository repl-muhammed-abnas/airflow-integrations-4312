# pylint: disable=wildcard-import unused-wildcard-import
from crl.user_import_brazil.config import *
from crl.user_import_brazil.mappers.user_import_mapper import user_import_mapper


instance = "trial"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriestrial01"
replicon_conn_id = "charlesriverlaboratoriestrial01_replicon_repliconadmin"
sftp_conn_id = "sftp_useast2"

log_filepath = "/CRLTrial/log"
payload_filepath = "/CRLTrial/Archive"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version= "" #_v1,_v2 etc
dag_postfix = f'{instance}{version}'

process_user_import_payload_dagid = f"crl_user_import_brazil_process_each_payload_child_{dag_postfix}"
process_users_dagid = f"crl_user_import_brazil_process_users_child_{dag_postfix}"
process_supervisor_dagid = f"crl_user_import_brazil_process_pending_supervisor_child_{dag_postfix}"
process_log_generation_dagid = f"crl_user_import_brazil_process_log_generation_child_{dag_postfix}"

process_groups_dagid = f"crl_user_import_brazil_process_groups_child_{dag_postfix}"
process_new_company_code_dagid = f"crl_user_import_brazil_process_new_company_code_child_{dag_postfix}"
process_new_locations_dagid = f"crl_user_import_brazil_process_new_location_child_{dag_postfix}"
process_new_buisness_unit_dagid = f"crl_user_import_brazil_process_new_buisness_unit_child_{dag_postfix}"
process_new_cost_center_dagid = f"crl_user_import_brazil_process_new_cost_center_child_{dag_postfix}"
process_new_department_dagid = f"crl_user_import_brazil_process_new_department_child_{dag_postfix}"

process_new_users_dagid = f"crl_user_import_brazil_process_new_users_child_{dag_postfix}"
process_update_users_dagid = f"crl_user_import_brazil_process_update_users_child_{dag_postfix}"
process_disable_users_dagid = f"crl_user_import_brazil_process_disable_users_child_{dag_postfix}"

process_timeoff_type_no_accrual_dagid = f"crl_user_import_brazil_process_timeoff_type_no_accrual_child_{dag_postfix}"
process_timeoff_type_assignment_new_user_dagid = f"crl_user_import_brazil_process_timeoff_type_new_user_child_{dag_postfix}"
process_timeoff_type_assignment_update_rehire_user_dagid = f"crl_user_import_brazil_process_timeoff_type_update_rehire_user_child_{dag_postfix}"

crl_user_import_bearer_token_var = f"crl_user_import_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_run_batch_task_{instance}'

USER_MAPPER = user_import_mapper

INTEGRATION_USERNAME = 'Admin, Replicon'
