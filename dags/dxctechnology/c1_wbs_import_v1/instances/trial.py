# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_wbs_import_v1.config import *
instance = ''
region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

sftp_conn_id = "rsftp-useast_for_testing"
input_filepath = "/DXC/C1WBS/input"
archive_filepath = "/DXC/C1WBS/archive"
log_filepath = "/DXC/C1WBS/logs"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

child_dag_id_program = "dxctechnology_c1_wbs_import_child_program_v1"
child_dag_id_cost_center = "dxctechnology_c1_wbs_import_child_cost_center_v1"
child_dag_id_project = "dxctechnology_c1_wbs_import_child_project_v1"
child_dag_id_client = "dxctechnology_c1_wbs_import_child_client_v1"
can_create_client_var_name = "dxctechnology_c1_wbs_can_create_client"
child_dag_id_icwbsnumber = "dxctechnology_c1_wbs_can_update_icwbsnumber_v1"
can_run_batch_task_var_name = f'dxctechnology_c1_wbs_import_{instance}_can_run_batch_task'
