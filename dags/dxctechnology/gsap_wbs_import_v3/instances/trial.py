# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_wbs_import_v3.config import *

instance = "trial"

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntGSAP'
sftp_conn_id = 'sftp_useast2'

input_filepath = "/Test/Inbound/GSAPWBS/Processing"
archive_filepath = "/Test/Inbound/GSAPWBS/Archive"
log_filepath = "/Test/Inbound/GSAPWBS/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'dxctechnology_gsap_wbs_import_master_{instance}_v3'
process_project_types_dagid = f'dxctechnology_gsap_wbs_import_child_process_project_type_{instance}_v3'
process_clients_dagid = f'dxctechnology_gsap_wbs_import_child_process_client_{instance}_v3'
process_wbs_dagid = f'dxctechnology_gsap_wbs_import_child_process_projects_{instance}_v3'
process_iwo_element_dagid = f'dxctechnology_gsap_wbs_import_child_process_iwo_element_{instance}_v3'
process_log_generation_dagid = f'dxctechnology_gsap_wbs_import_child_process_log_generation_{instance}_v3'
process_blob_dagid = f'dxctechnology_gsap_wbs_import_child_process_blob_{instance}_v3'
process_child_projects_dagid = f'dxctechnology_gsap_wbs_import_child_process_child_projects_{instance}_v3'
process_tasks_by_level_dagid = f'dxctechnology_gsap_wbs_import_child_process_task_by_level_{instance}_v3'
process_create_task_dagid = f'dxctechnology_gsap_wbs_import_child_process_create_task_{instance}_v3'

process_diwo_master_dagid = f'dxctechnology_gsap_wbs_import_process_diwo_master_{instance}_v3'

can_run_batch_task_var_name = f'dxctechnology_gsap_wbs_import_{instance}_can_run_batch_task'

disable=True

disabled=True
