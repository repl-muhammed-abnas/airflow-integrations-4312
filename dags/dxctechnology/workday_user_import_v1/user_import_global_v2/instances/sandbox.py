# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.workday_user_import_v1.user_import.config import *
from dxctechnology.workday_user_import_v1.user_import.mappers.master_mapper import MAPPER
instance = "sandbox"

version = "v2"
ia_version = "v1"
no_accrual_version = "v1"

environment = "pre-production"
can_run_batch_task_var_name_global = f"dxctechnology_workday_user_import_global_can_run_batch_task_variable_{instance}"

company_key = "dxcsandbox"
replicon_conn_id = "replicon_dxcsandbox_x.workday_1"
sftp_conn_id = "sftp_dxcsandbox_628172_Workday"

pgp_conn_id = f"dxctechnology_workday_user_import_pgp_connection_{instance}"

input_file_path = "/Test/Input"
archive_file_path = "/Test/Archives"
log_file_path = "/Test/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_emails = "{{ var.value.dagrun_internal_testing_email }}"

can_decrypt_file_var_name = f'dxctechnology_workday_user_sync_can_decrypt_file_{instance}'

DXC_WORKDAY_USER_SYNC_USER_MAPPER = MAPPER

workday_user_import_global_v2_users_add_user_child_dag = f"dxctechnology_workday_user_import_global_users_add_user_child_{instance}_{version}"
workday_user_import_global_v2_users_add_user_timeoff_process_child_dag = f"dxctechnology_workday_user_import_global_users_add_user_timeoff_process_child_{instance}_{version}"
workday_user_import_global_v2_users_update_user_timeoff_process_child_dag = f"dxctechnology_workday_user_import_global_users_update_user_timeoff_process_child_{instance}_{version}"
workday_user_import_global_v2_users_add_user_timeoff_process_child_for_canada_dag = f"dxctechnology_workday_user_import_global_users_add_user_timeoff_process_child_for_canada_{instance}_{version}"
workday_user_import_global_v2_users_update_user_timeoff_process_child_dag_disable = f"dxctechnology_workday_user_import_global_users_update_user_timeoff_process_child_for_disable_user_{instance}_{version}"
workday_user_import_global_v2_users_update_user_child_dag = f"dxctechnology_workday_user_import_global_users_update_user_child_{instance}_{version}"

process_time_off_accrual = f"dxctechnology_workday_user_sync_timeoff_assignment_policy_update_for_no_accrual_child_{instance}_{no_accrual_version}"

workday_user_import_ia_zero_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_ia_zero_timeoff_assignment_child_{instance}_{ia_version}"
workday_user_import_ia_one_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_ia_one_timeoff_assignment_child_{instance}_{ia_version}"

# Cleanup child DAG ID for disabled users
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_{instance}_v2"

# Overrides: bump max_active_runs to 10 for this environment (base defaults in config.py are unchanged, so production is not affected)
global_add_user_max_active_runs = 10
global_add_user_timeoff_assignment_max_active_runs = 10
global_update_user_max_active_runs = 10
global_update_user_timeoff_assignment_max_active_runs = 10
