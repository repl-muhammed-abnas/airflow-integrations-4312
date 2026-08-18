# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_wbs_import_v4.config import *

instance = 'sandbox'
region = 'us-east-2'
environment = 'pre-production'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntC1'

sftp_conn_id = "dxcsandbox-sftp-628172_C1"
input_filepath = "/Test/Inbound/C1WBSMaster/Input"
archive_filepath = "/Test/Inbound/C1WBSMaster/Archive"
log_filepath = "/Test/Inbound/C1WBSMaster/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id= f'dxctechnology_c1_wbs_import_master_{instance}_v4'
child_dag_id_program = f"dxctechnology_c1_wbs_import_child_program_{instance}_v4"
child_dag_id_cost_center = f"dxctechnology_c1_wbs_import_child_cost_center_{instance}_v4"
child_dag_id_project = f"dxctechnology_c1_wbs_import_child_project_{instance}_v4"
child_dag_id_client = f"dxctechnology_c1_wbs_import_child_client_{instance}_v4"
child_dag_id_icwbsnumber = f"dxctechnology_c1_wbs_can_update_icwbsnumber_{instance}_v4"

can_create_client_var_name = f"dxctechnology_c1_wbs_can_create_client_{instance}"
can_run_batch_task_var_name = f'dxctechnology_c1_wbs_import_{instance}_can_run_batch_task'
