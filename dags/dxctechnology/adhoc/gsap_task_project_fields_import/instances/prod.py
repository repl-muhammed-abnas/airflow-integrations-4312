# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.adhoc.gsap_task_project_fields_import.config import *

environment = 'production'

company_key = 'dxctechnology'
instance = "production"

replicon_conn_id = 'dxctechnology_replicon_RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"


input_filepath = "/Inbound/Tasks/Data Import/Input"
archive_filepath = "/Inbound/Tasks/Data Import/Archives"
log_filepath = "/Inbound/Tasks/Data Import/Logs"


can_run_batch_task_var_name = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task'

child_dag_sync_gsap_task_system_level = 50
