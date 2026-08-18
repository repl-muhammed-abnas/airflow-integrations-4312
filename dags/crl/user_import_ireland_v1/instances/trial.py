# pylint: disable=wildcard-import unused-wildcard-import
from crl.user_import_ireland_v1.config import *
from crl.user_import_ireland_v1.mappers.user_import_mapper import user_import_mapper
from crl.user_import_ireland_v1.mappers.annual_leave_placeholder_mapper import annual_leave_placeholder_timeoff_types
from crl.user_import_ireland_v1.mappers.sick_leave_placeholder_mapper import sick_leave_placeholder_timeoff_types
from crl.user_import_ireland_v1.mappers.personal_leave_placeholder_mapper import personal_leave_placeholder_timeoff_types
instance = "trial"
environment = "pre-production"

dagid_suffix = f"_{instance}_v1" # for new versions add _v1, _v2, _v3 etc at the end of the instance name

company_key = "CharlesRiverLaboratoriestrial01"
replicon_conn_id = "charlesriverlaboratoriestrial01_replicon_repliconadmin"
sftp_conn_id = "sftp_useast2"

log_filepath = "/CRLTrial/log"
payload_filepath = "/CRLTrial/Archive"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

process_user_import_payload_dagid = f"crl_user_import_ireland_process_each_payload_child{dagid_suffix}"
process_users_dagid = f"crl_user_import_ireland_process_users_child{dagid_suffix}"
process_supervisor_dagid = f"crl_user_import_ireland_process_pending_supervisor_child{dagid_suffix}"
process_log_generation_dagid = f"crl_user_import_ireland_process_log_generation_child{dagid_suffix}"

process_groups_dagid = f"crl_user_import_ireland_process_groups_child{dagid_suffix}"
process_new_company_code_dagid = f"crl_user_import_ireland_process_new_company_code_child{dagid_suffix}"
process_new_locations_dagid = f"crl_user_import_ireland_process_new_location_child{dagid_suffix}"
process_new_buisness_unit_dagid = f"crl_user_import_ireland_process_new_buisness_unit_child{dagid_suffix}"
process_new_cost_center_dagid = f"crl_user_import_ireland_process_new_cost_center_child{dagid_suffix}"
process_new_department_dagid = f"crl_user_import_ireland_process_new_department_child{dagid_suffix}"

process_new_users_dagid = f"crl_user_import_ireland_process_new_users_child{dagid_suffix}"
process_update_users_dagid = f"crl_user_import_ireland_process_update_users_child{dagid_suffix}"
process_disable_users_dagid = f"crl_user_import_ireland_process_disable_users_child{dagid_suffix}"

process_timeoff_type_no_accrual_dagid = f"crl_user_import_ireland_process_timeoff_type_no_accrual_child{dagid_suffix}"
process_timeoff_type_assignment_new_user_dagid = f"crl_user_import_ireland_process_timeoff_type_new_user_child{dagid_suffix}"
process_timeoff_type_assignment_vacation_new_user_dagid = f"crl_user_import_ireland_process_timeoff_type_vacation_new_user_child{dagid_suffix}"
process_timeoff_type_assignment_update_rehire_user_dagid = f"crl_user_import_ireland_process_timeoff_type_update_rehire_user_child{dagid_suffix}"


disable_user_master_dagid = f'crl_user_import_ireland_disable_future_enddate_user_master_{instance}'
disable_future_enddate_user_child_dagid = f'crl_user_import_ireland_disable_future_enddate_user_child_{instance}'

crl_user_import_bearer_token_var = f"crl_user_import_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_run_batch_task_{instance}'

USER_MAPPER = user_import_mapper
ANNUAL_TO_PLACEHOLDER = annual_leave_placeholder_timeoff_types
SICK_TO_PLACEHOLDER = sick_leave_placeholder_timeoff_types
PERSONAL_TO_PLACEHOLDER = personal_leave_placeholder_timeoff_types
