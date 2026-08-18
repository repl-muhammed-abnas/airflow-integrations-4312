# pylint: disable=wildcard-import unused-wildcard-import
from tpg.user_import.config import *

instance = "uat"
environment = 'pre-production'
company_key = 'AngeloGordontrial02'
replicon_conn_id = 'tpg_replicon_repadmin'
sftp_conn_id = 'sftp_tpg_670250'

tenant_email = 'sprouty@tpg.com,mgadison@tpg.com,dford@tpg.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

input_filepath = '/UAT User Import/input'
reference_filepath = '/UAT User Import/reference'
archive_filepath = '/UAT User Import/archive'
log_filepath = '/UAT User Import/logs'

tpg_user_import_master = f'tpg_user_import_master_{instance}'
process_groups = f'tpg_user_import_groups_child_{instance}'
process_new_locations = f'tpg_user_import_locations_child_{instance}'
process_new_departments = f'tpg_user_import_departments_child_{instance}'
process_new_employee_types = f'tpg_user_import_employee_types_child_{instance}'
process_new_divisions = f'tpg_user_import_divisions_child_{instance}'
process_users = f'tpg_user_import_process_users_child_{instance}'
processs_supervisor = f'tpg_user_import_process_pending_supervisor_child_{instance}'
process_new_users = f'tpg_user_import_process_new_users_child_{instance}'
process_update_users = f'tpg_user_import_process_update_users_child_{instance}'
process_log_generation = f'tpg_user_import_process_log_generation_child_{instance}'

can_run_batch_task = f'tpg_user_import_can_run_batch_task_{instance}'
