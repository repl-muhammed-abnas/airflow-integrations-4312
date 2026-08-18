# pylint: disable=wildcard-import unused-wildcard-import
from sigroup.user_import.config import *
from sigroup.user_import.mapper.user_import_mapper_v2 import user_import_new_mapper_v2
instance = "trial"
environment = "qa"

company_key = "sigrouptrial01"
replicon_conn_id = "sigrouptrial01_replicon_adminuser"
sftp_conn_id = "sftp_useast2"
pgp_conn_id = f"pgp_sigroup_user_import_{instance}"
input_filepath = "/sigroup/user_import/input"
archive_filepath = "/sigroup/user_import/archive"
log_filepath = "/sigroup/user_import/log/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sigroup_user_import_decrypt_var = f"sigrouptrial01_user_import_decrypt_variable_{instance}"

sigroup_legal_employers_dag_id = f"sigroup_user_import_legal_employers_child_{instance}"
sigroup_locations_dag_id = f"sigroup_user_import_locations_child_{instance}"
sigroup_paygroups_dag_id = f"sigroup_user_import_paygroups_child_{instance}"
sigroup_departments_dag_id = f"sigroup_user_import_departments_child_{instance}"
sigroup_costcenters_dag_id = f"sigroup_user_import_costcenters_child_{instance}"
sigroup_business_units_dag_id = f"sigroup_user_import_business_units_child_{instance}"
sigroup_states_dag_id = f"sigroup_user_import_states_child_{instance}"
sigroup_cities_dag_id = f"sigroup_user_import_cities_child_{instance}"
sigroup_coefficient_levels_dag_id = f"sigroup_user_import_coefficient_levels_child_{instance}"
sigroup_elderly_allowance_dag_id = f"sigroup_user_import_elderly_allowance_child_{instance}"
sigroup_timecode_dag_id = f"sigroup_user_import_timecode_levels_child_{instance}"
sigroup_cba_appendix_levels_dag_id = f"sigroup_user_import_cba_appendix_levels_child_{instance}"
sigroup_tariff_classification_dag_id = f"sigroup_user_import_tariff_classification_levels_child_{instance}"
sigroup_step_information_dag_id = f"sigroup_user_import_step_information_child_{instance}"
sigroup_disable_user_dag_id = f"sigroup_user_import_disable_user_child_{instance}"
sigroup_add_user_dag_id = f"sigroup_user_import_add_user_child_{instance}"
sigroup_update_user_dag_id = f"sigroup_user_import_update_user_child_{instance}"
sigroup_user_import_timeoff_type_for_add_user = f"sigroup_user_import_timeoff_type_for_add_user_child_{instance}"
sigroup_user_import_disable_user_blank_timeoff_policy = f"sigroup_user_import_disable_user_blank_timeoff_policy_child_{instance}"
sigroup_process_log_generation_dagid = f"sigroup_user_import_process_log_generation_child_{instance}"
sigroup_process_supervisor_dagid = f"sigroup_user_import_process_supervisor_child_{instance}"
sigroup_user_import_timeoff_type_for_update_user=f"sigroup_user_import_timeoff_type_for_update_user_child_{instance}"
sigroup_user_import_timeoff_type_for_rehire_user=f"sigroup_user_import_timeoff_type_for_rehire_user_child_{instance}"
sigroup_user_import_timeoff_type_for_update_user_payout_user=f"sigroup_user_import_timeoff_type_for_update_user_payout_child_{instance}"
sigroup_valid_user_dag_id = f"sigroup_user_import_valid_user_child_{instance}"
sigroup_batch_task_flag = f"sigroup_batch_task_variable_{instance}"
USER_IMPORT_MAPPER=user_import_new_mapper_v2