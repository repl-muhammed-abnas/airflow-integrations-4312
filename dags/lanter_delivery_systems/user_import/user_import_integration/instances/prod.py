# pylint: disable=wildcard-import unused-wildcard-import
from lanter_delivery_systems.user_import.user_import_integration.config import *

instance = "production"
environment = "production"

company_key = "lds"

replicon_conn_id = "lds_replicon_admin"
sftp_conn_id = "client_sftp_lds_replicon"
replicon_sftp_conn_id = 'replicon_sftp_lds_676481'

splitfile_input_filepath = "/downloads/Replicon"
process_users_input_filepath = "/Production/UserImport/NewUser"
disable_users_input_filepath = "/Production/UserImport/DisabledUsers"
archive_filepath = "/Production/UserImport/Archive"
log_filepath = "/Production/UserImport/LogFile"

s3_reference_filepath = "/LDS/Reference"
s3_reference_archive_filepath = "/LDS/Reference/Archive"

tenant_email = "Jacob.Grass@rubinbrown.com,AWelden@lanterds.com,rstone@ctr.lanterds.com,hr@lanterds.com,recruiting@lanterds.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
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
