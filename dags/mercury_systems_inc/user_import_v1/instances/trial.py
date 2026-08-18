from mercury_systems_inc.user_import_v1.config import *

instance = "trial"
company_key = "MercurySystemsIncSB"

replicon_conn_id = "mercurysystemsincsb_replicon_repliconint"

sftp_conn_id = "sftp_useast"

sftp_input_filepath = "/mercury_systems_inc/input"
sftp_archive_filepath = "/mercury_systems_inc/archive"
sftp_log_filepath = "/mercury_systems_inc/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"mercury_systems_inc_user_import_can_run_batch_task_var_name_{instance}"

version = "_v1"

master_dag_id = f"mercury_systems_inc_user_import_master_{instance}{version}"

process_groups_dagid = f"mercury_systems_inc_user_import_process_groups_child_{instance}{version}"
process_new_location_add_dagid = f"mercury_systems_inc_user_import_process_new_location_add_child_{instance}{version}"
process_new_department_dagid = f"mercury_systems_inc_user_import_process_new_department_child_{instance}{version}"

process_each_user_payload_dagid = f"mercury_systems_inc_user_import_process_each_user_payload_child_{instance}{version}"
process_disable_user_dagid = f"mercury_systems_inc_user_import_process_disable_users_child_{instance}{version}"
process_new_user_dagid = f"mercury_systems_inc_user_import_process_new_users_child_{instance}{version}"
process_update_user_dagid = f"mercury_systems_inc_user_import_process_update_users_child_{instance}{version}"
process_rehire_user_dagid = f"mercury_systems_inc_user_import_process_rehire_users_child_{instance}{version}"

process_log_generation_dagid = f"mercury_systems_inc_user_import_process_log_generation_child_{instance}{version}"

process_stop_accrual_for_timeoff_types = f"mercury_systems_inc_user_import_process_stop_accrual_for_timeoff_types_types_child_{instance}{version}"
process_existing_eligible_timeoff_types_for_rehire_users_dagid = f"mercury_systems_inc_user_import_process_existing_eligible_timeoff_types_for_rehire_users_child_{instance}{version}"

disabled = True
