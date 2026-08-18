# pylint: disable=wildcard-import unused-wildcard-import
from cohnreznick.user_sync_v1.config import *
from cohnreznick.user_sync_v1.mapper.payrule_mapper import payrule_mapper
from cohnreznick.user_sync_v1.mapper.cost_center_mapper import cost_center_mapper
from cohnreznick.user_sync_v1.mapper.timesheet_template_punch_policy_mapper import timesheet_template_punch_policy_mapper

instance = "uat"
environment = "pre-production"

company_key = "cohnreznicktrial01"

replicon_conn_id = "cohnreznicktrial01_replicon_admin"
sftp_conn_id = "sftp_cohnreznicktrial01_640189"
pgp_conn_id = "pgp_cohnreznicktrial01_user_import"
can_decrypt_file_var_name = f"cohnreznicktrial01_user_import_can_decrypt_file_{instance}"

input_filepath = "/Trial/Import/User Sync/Input"
archive_filepath = "/Trial/Import/User Sync/Archive"
log_filepath = "/Trial/Import/User Sync/Log"
reference_file ="/Trial/Import/User Sync/Reference/user_sync_reference_file.csv"

tenant_email = "Sandra.Lopez-Silvero@CohnReznick.com,Savya.Jangili@CohnReznick.com,Replicon.IntegrationErrors@CohnReznick.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'cohnreznick_user_sync_master_{instance}_v1'
process_groups_dag_id = f'cohnreznick_user_sync_child_process_groups_{instance}_v1'
process_new_locations = f'cohnreznick_user_sync_child_process_locations_{instance}_v1'
process_new_departments = f'cohnreznick_user_sync_child_process_departments_{instance}_v1'
process_new_servicecenters = f'cohnreznick_user_sync_child_process_servicecenters_{instance}_v1'
process_new_costcenters = f'cohnreznick_user_sync_child_process_costcenters_{instance}_v1'
process_new_divisions = f'cohnreznick_user_sync_child_process_divisions_{instance}_v1'
process_component_company = f'cohnreznick_user_sync_child_process_component_company_{instance}_v1'
process_users = f'cohnreznick_user_sync_child_process_users_{instance}_v1'
process_new_users = f'cohnreznick_user_sync_child_process_new_users_{instance}_v1'
process_update_users = f'cohnreznick_user_sync_child_process_update_users_{instance}_v1'
process_log_generation = f'cohnreznick_user_sync_child_process_log_generation_{instance}_v1'

can_run_batch_task_var_name = f'cohnreznick_user_sync_run_batch_task_{instance}'

PAYRULE_MAPPER = payrule_mapper
COSTCENTER_MAPPER = cost_center_mapper
TIMESHEET_TEMPLATE_PUNCH_POLICY_MAPPER = timesheet_template_punch_policy_mapper
