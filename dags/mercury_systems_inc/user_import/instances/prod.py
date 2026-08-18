from mercury_systems_inc.user_import.config import *

environment = 'production'

instance = "prod"
company_key = "MercurySystemsInc"

replicon_conn_id = "mercurysystemsinc_replicon_repliconint"

sftp_conn_id = "sftp_mercury_systems_inc"

sftp_input_filepath = "/Production/ADPHRIS/Input"
sftp_archive_filepath = "/Production/ADPHRIS/Processed"
sftp_log_filepath = "/Production/ADPHRIS/Log"

tenant_email = "RepliconAdmin@mrcy.com"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

can_run_batch_task_var_name = f"mercury_systems_inc_user_import_can_run_batch_task_var_name_{instance}"

master_dag_id = f"mercury_systems_inc_user_import_master_{instance}"

process_groups_dagid = f"mercury_systems_inc_user_import_process_groups_child_{instance}"
process_new_location_add_dagid = f"mercury_systems_inc_user_import_process_new_location_add_child_{instance}"
process_new_department_dagid = f"mercury_systems_inc_user_import_process_new_department_child_{instance}"

process_each_user_payload_dagid = f"mercury_systems_inc_user_import_process_each_user_payload_child_{instance}"
process_disable_user_dagid = f"mercury_systems_inc_user_import_process_disable_users_child_{instance}"
process_new_user_dagid = f"mercury_systems_inc_user_import_process_new_users_child_{instance}"
process_update_user_dagid = f"mercury_systems_inc_user_import_process_update_users_child_{instance}"

process_log_generation_dagid = f"mercury_systems_inc_user_import_process_log_generation_child_{instance}"

process_stop_accrual_for_timeoff = f"mercury_systems_inc_user_import_process_stop_accrual_for_timeoff_child_{instance}"
