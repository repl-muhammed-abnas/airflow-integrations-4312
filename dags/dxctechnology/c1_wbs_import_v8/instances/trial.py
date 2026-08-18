# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_wbs_import_v8.config import *

instance = 'trial'
region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01_replicon_RepliconIntC1'

sftp_conn_id = "sftp_useast2"
input_filepath = "/Test/Inbound/C1WBS/Input"
archive_filepath = "/Test/Inbound/C1WBS/Archive"
log_filepath = "/Test/Inbound/C1WBS/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id= f'dxctechnology_c1_wbs_import_master_{instance}_v8'
child_dag_id_program = f"dxctechnology_c1_wbs_import_child_program_{instance}_v8"
child_dag_id_cost_center = f"dxctechnology_c1_wbs_import_child_cost_center_{instance}_v8"
child_dag_id_project = f"dxctechnology_c1_wbs_import_child_project_{instance}_v8"
child_dag_id_client = f"dxctechnology_c1_wbs_import_child_client_{instance}_v8"
child_dag_id_icwbsnumber = f"dxctechnology_c1_wbs_can_update_icwbsnumber_{instance}_v8"

can_create_client_var_name = f"dxctechnology_c1_wbs_can_create_client_{instance}"
can_run_batch_task_var_name = f'dxctechnology_c1_wbs_import_{instance}_can_run_batch_task'
