# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_import_project_fields_v1.config import *

environment = 'pre-production'

company_key = 'dxcsandbox'
instance = "sandbox"

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = "/Inbound/Tasks/Processing"
archive_filepath = "/Inbound/Tasks/Archives"
log_filepath = "/Inbound/Tasks/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task'

# dag_ids
move_to_processing_dagid = f'dxctechnology_gsap_project_field_task_move_file_processing_master_{instance}_v1'
main_dag = f'dxctechnology_gsap_project_field_task_import_master_{instance}_v1'
process_each_child_wbs = f'dxctechnology_gsap_project_field_task_import_sync_child_wbs_{instance}_v1'
process_each_wbs = f'dxctechnology_gsap_project_field_task_import_process_each_wbs_{instance}_v1'
log_generation = f'dxctechnology_gsap_project_field_task_import_child_process_log_generation_{instance}_v1'
add_gsap_task = f'dxctechnology_gsap_project_field_task_import_add_gsap_task_{instance}_v1'

disabled=True
