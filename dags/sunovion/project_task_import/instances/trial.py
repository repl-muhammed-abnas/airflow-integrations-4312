# pylint: disable=wildcard-import unused-wildcard-import
from sunovion.project_task_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'sunovionafmig'
input_filepath = '/Sunovion/557911sbebs/Processing/Input'
archive_filepath = '/Sunovion/557911sbebs/Processing/Archive'
archive_input_filepath = '/Sunovion/557911sbebs/Processing/Archive'
reference_file_path = '/Sunovion/557911sbebs/Processing/Reference'
log_filepath = '/Sunovion/557911sbebs/Processing/Logs'

replicon_conn_id = 'sunovionafmig_replicon_admin'
sftp_conn_id = 'rsftp-useast_for_testing'
sftp_conn_id2 = 'rsftp-useast_for_testing'

can_run_batch_task_var_name = f"sunovion_project_import_can_run_batch_task_{instance}"
disabled = True
