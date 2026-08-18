# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_import_v3.config import *

instance = 'trial'
company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01-replicon-RepliconIntCompass'
sftp_conn_id = 'sftp_useast2'

input_filepath = '/Test/Inbound/CompassWBS/Input'
archive_filepath = '/Test/Inbound/CompassWBS/Archive'
log_filepath = '/Test/Inbound/CompassWBS/Logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxc_compass_wbs_import_can_run_batch_task_{instance}'


main_dagid = f'dxctechnology_compass_wbs_import_master_{instance}_v3'
inactive_project_dagid = f'dxctechnology_compass_wbs_import_child_inactive_project_{instance}_v3'
active_project_dagid = f'dxctechnology_compass_wbs_import_child_active_project_{instance}_v3'
process_program_dagid = f'dxctechnology_compass_wbs_import_child_program_{instance}_v3'
process_client_dagid = f'dxctechnology_compass_wbs_import_child_client_{instance}_v3'
process_child_project_dagid = f'dxctechnology_compass_wbs_import_child_process_child_project_{instance}_v3'
disabled = True
