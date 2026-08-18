# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.workday_user_import_v1.disable_user.config import *

instance = "trial"
version = '_v1' # _v1,_v2,_v3 etc.

environment = "pre-production"

company_key = "dxctrial01"
replicon_conn_id = "dxctrial01_replicon_x.replicon.workday1"

can_run_batch_task_var_name_disable_user = f"dxctechnology_workday_user_import_v1_disable_user_can_run_batch_task_variable_{instance}"

disable_user_master_dag_id = f"dxctechnology_workday_user_sync_disable_users_master_{instance}{version}"
disable_user_process_each_user_dag_id = f"dxctechnology_workday_user_sync_disable_users_process_user_child_{instance}{version}"

# New child DAG ID for deletion logic
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_{instance}{version}"

process_time_off_accrual = f"dxctechnology_workday_disable_user_timeoff_assignment_policy_update_for_no_accrual_child_{instance}{version}"

# User Timesheet Deletion DAG ID
user_timesheet_deletion_dag_id = f"dxctechnology_workday_user_sync_timesheet_deletion_{instance}{version}"

# Overrides: bump max_active_runs to 10 for this environment (base defaults in config.py are unchanged, so production is not affected)
process_time_off_accrual_max_active_runs = 10
