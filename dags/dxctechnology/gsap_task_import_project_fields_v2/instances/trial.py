# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_import_project_fields_v2.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "trial"
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntGSAP'

sftp_conn_id = "sftp_useast2"

input_filepath = "/Trial/Inbound/gsapTask/Processing"
archive_filepath = "/Trial/Inbound/gsapTask/Archive"
log_filepath = "/Trial/Inbound/gsapTask/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

reprocess_not_found_wbs_email = '{{ var.value.dagrun_internal_testing_email }}'
can_run_batch_task_var_name = f'dxctechnology_gsap_task_import_project_fields_v2_{instance}_can_run_batch_task'

# dag_ids
move_to_processing_dagid = f'dxctechnology_gsap_project_field_task_move_file_processing_master_{instance}_v2'
main_dag = f'dxctechnology_gsap_project_field_task_import_master_{instance}_v2'
process_each_child_wbs = f'dxctechnology_gsap_project_field_task_import_sync_child_wbs_{instance}_v2'
process_each_wbs = f'dxctechnology_gsap_project_field_task_import_process_each_wbs_{instance}_v2'
log_generation = f'dxctechnology_gsap_project_field_task_import_child_process_log_generation_{instance}_v2'
add_gsap_task = f'dxctechnology_gsap_project_field_task_import_add_gsap_task_{instance}_v2'

disable=True

disabled=True
