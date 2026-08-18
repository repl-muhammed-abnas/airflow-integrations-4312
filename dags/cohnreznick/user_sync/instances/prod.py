# pylint: disable=wildcard-import unused-wildcard-import
from cohnreznick.user_sync.config import *

instance = "prod"
environment = "production"

company_key = "cohnreznick"

replicon_conn_id = "cohnreznick_replicon_repliconint.userimport"
sftp_conn_id = "sftp_cohnreznick_640189"
pgp_conn_id = "pgp_cohnreznick_user_import"
can_decrypt_file_var_name = f"cohnreznick_user_import_can_decrypt_file_{instance}"

input_filepath = "/Production/Import/User sync/Input"
archive_filepath = "/Production/Import/User sync/Archive"
log_filepath = "/Production/Import/User sync/Log"
reference_file ="/Production/Import/User Sync/Reference/user_sync_reference_file.csv"

tenant_email = "Payroll@CohnReznick.com,Dorthell.Little@CohnReznick.com,Sandra.Lopez-Silvero@CohnReznick.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
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
