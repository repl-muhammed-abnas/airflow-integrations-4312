# pylint: disable=wildcard-import unused-wildcard-import
from cohnreznick.user_sync.config import *

instance = "trial"
environment = "pre-production"

company_key = "cohnreznicktrial01"

replicon_conn_id = "cohnreznicktrial01_replicon_admin"
sftp_conn_id = "sftp_useast2"
pgp_conn_id = "pgp_cohnreznicktrial01_user_import"
can_decrypt_file_var_name = f"cohnreznicktrial01_user_import_can_decrypt_file_{instance}"

input_filepath = "/Cohnreznick/Input"
archive_filepath = "/Cohnreznick/Archive"
log_filepath = "/Cohnreznick/Logs"
reference_file ="/Cohnreznick/Reference/user_sync_reference_file.csv"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'cohnreznick_user_sync_master_{instance}'
process_groups_dag_id = f'cohnreznick_user_sync_child_process_groups_{instance}'
process_new_locations = f'cohnreznick_user_sync_child_process_locations_{instance}'
process_new_departments = f'cohnreznick_user_sync_child_process_departments_{instance}'
process_new_servicecenters = f'cohnreznick_user_sync_child_process_servicecenters_{instance}'
process_new_costcenters = f'cohnreznick_user_sync_child_process_costcenters_{instance}'
process_new_divisions = f'cohnreznick_user_sync_child_process_divisions_{instance}'
process_component_company = f'cohnreznick_user_sync_child_process_component_company_{instance}'
process_users = f'cohnreznick_user_sync_child_process_users_{instance}'
process_new_users = f'cohnreznick_user_sync_child_process_new_users_{instance}'
process_update_users = f'cohnreznick_user_sync_child_process_update_users_{instance}'
process_log_generation = f'cohnreznick_user_sync_child_process_log_generation_{instance}'

can_run_batch_task_var_name = f'cohnreznick_user_sync_run_batch_task_{instance}'

disabled=True
