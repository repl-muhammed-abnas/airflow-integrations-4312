# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_wbs_import_v2.config import *
instance = 'trial'
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

child_dag_id_program = f"dxctechnology_c1_wbs_import_child_program_v2_{instance}"
child_dag_id_cost_center = f"dxctechnology_c1_wbs_import_child_cost_center_v2_{instance}"
child_dag_id_project = f"dxctechnology_c1_wbs_import_child_project_v2_{instance}"
child_dag_id_client = f"dxctechnology_c1_wbs_import_child_client_v2_{instance}"
can_create_client_var_name = f"dxctechnology_c1_wbs_can_create_client_{instance}"
child_dag_id_icwbsnumber = f"dxctechnology_c1_wbs_can_update_icwbsnumber_v2_{instance}"
can_run_batch_task_var_name = f'dxctechnology_c1_wbs_import_{instance}_can_run_batch_task'
