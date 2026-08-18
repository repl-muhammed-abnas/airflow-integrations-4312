# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.workday_user_import.disable_user.config import *

instance = "trial"

environment = "pre-production"

company_key = "dxctrial01"
replicon_conn_id = "dxctrial01_replicon_x.replicon.workday1"

can_run_batch_task_var_name = f"dxctechnology_workday_user_import_disable_user_can_run_batch_task_variable_{instance}"

disable_user_master_dag_id = f"dxctechnology_workday_user_sync_disable_users_master_{instance}"
disable_user_process_each_user_dag_id = f"dxctechnology_workday_user_sync_disable_users_process_user_child_{instance}"

process_time_off_accrual = f"dxctechnology_workday_disable_user_timeoff_assignment_policy_update_for_no_accrual_child_{instance}"
