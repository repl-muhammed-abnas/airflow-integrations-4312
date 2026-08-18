# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_wbs_import_v1.config import *

instance = 'production'
region = 'us-east-2'
environment = 'production'
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntC1'

sftp_conn_id = "sftp_dxctechnology_c1"
input_filepath = "/Production/Inbound/C1WBSMaster/Input"
archive_filepath = "/Production/Inbound/C1WBSMaster/Archive"
log_filepath = "/Production/Inbound/C1WBSMaster/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# configured here as this is already getting used in the master_dag
child_dag_id_program = f"dxctechnology_c1_wbs_import_child_program_v1_{instance}"
child_dag_id_cost_center = f"dxctechnology_c1_wbs_import_child_cost_center_v1_{instance}"
child_dag_id_project = f"dxctechnology_c1_wbs_import_child_project_v1_{instance}"
child_dag_id_client = f"dxctechnology_c1_wbs_import_child_client_v1_{instance}"
can_create_client_var_name = f"dxctechnology_c1_wbs_can_create_client_{instance}"
child_dag_id_icwbsnumber = f"dxctechnology_c1_wbs_can_update_icwbsnumber_v1_{instance}"
can_run_batch_task_var_name = f'dxctechnology_c1_wbs_import_{instance}_can_run_batch_task'
