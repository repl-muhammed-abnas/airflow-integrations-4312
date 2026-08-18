# pylint: disable=wildcard-import unused-wildcard-import
from sunovion.project_task_import.config import *

instance = 'production'
environment = 'production'
company_key = 'sunovion'
input_filepath = '/OracleToReplicon'
archive_input_filepath = '/OracleToReplicon/Archive'
archive_filepath = '/557911PDEBS/Processing/Archive'
reference_file_path = '/557911PDEBS/Processing/Reference'
log_filepath = '/557911PDEBS/Processing/Logs'

replicon_conn_id = 'sunovion_replicon_admin'
sftp_conn_id = 'sftp_sunovion_557911_workato_useast'
sftp_conn_id2 = 'sftp_sunovion_557911PDEBS'

can_run_batch_task_var_name = f"sunovion_project_import_can_run_batch_task_{instance}"
