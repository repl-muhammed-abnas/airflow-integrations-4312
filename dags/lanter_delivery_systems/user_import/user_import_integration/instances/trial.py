# pylint: disable=wildcard-import unused-wildcard-import
from lanter_delivery_systems.user_import.user_import_integration.config import *

instance = "trial"
environment = "pre-production"

company_key = "ldstrial01"

replicon_conn_id = "ldstrial01_replicon_admin"
sftp_conn_id = "sftp_useast2"
replicon_sftp_conn_id = 'sftp_useast2'

splitfile_input_filepath = "/lanter/Input"
process_users_input_filepath = "/lanter/NewUser"
disable_users_input_filepath = "/lanter/Disable"
archive_filepath = "/lanter/Archive"
log_filepath = "/lanter/Logs"

s3_reference_filepath = "/LDSTrial01/Reference"
s3_reference_archive_filepath = "/LDSTrial01/Reference/Archive"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

split_files_master_dagid = f'lds_user_import_split_files_for_processing_master_{instance}'
master_dagid = f'lds_user_import_master_{instance}'
process_groups_dagid = f'lds_user_import_process_groups_child_{instance}'
process_new_locations_dagid = f'lds_user_import_process_locations_child_{instance}'
process_new_departments_dagid = f'lds_user_import_process_departments_child_{instance}'
process_new_employee_types_dagid = f'lds_user_import_process_employee_types_child_{instance}'
process_users_dagid = f'lds_user_import_process_users_child_{instance}'
processs_supervisor_dagid = f'lds_user_import_process_pending_supervisor_child_{instance}'
process_new_users_dagid = f'lds_user_import_process_new_users_child_{instance}'
process_update_users_dagid = f'lds_user_import_process_update_users_child_{instance}'
process_log_generation_dagid = f'lds_user_import_process_log_generation_child_{instance}'

can_run_batch_task_var_name = f'lds_user_import_can_run_batch_task_{instance}'
disabled = True
