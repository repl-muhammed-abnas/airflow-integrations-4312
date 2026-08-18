# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.workday_user_import.decrypt_file.config import *

instance = "sandbox"

environment = "pre-production"
can_run_batch_task_var_name = f"dxctechnology_workday_user_import_can_run_batch_task_variable_{instance}"

company_key = "dxcsandbox"
replicon_conn_id = "dxcsandbox_replicon_x.replicon.workday1"
sftp_conn_id = "sftp_dxcsandbox_628172_Workday"

pgp_conn_id = f"dxctechnology_workday_user_import_pgp_connection_{instance}"

input_file_path = "/Test/Input"
archive_file_path = "/Test/Archives"
log_file_path = "/Test/Logs"
