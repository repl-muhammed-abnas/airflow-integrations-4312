region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

cost_center_dag_max_active_runs = 128
program_dag_max_active_runs = 128
project_dag_max_active_runs = 128
client_dag_max_active_runs = 128

dag_max_active_tasks = 128
sftp_conn_id = "repliconsftp"
input_filepath = "/DXC/C1WBS/input"
archive_filepath = "/DXC/C1WBS/archive"
log_filepath = "/DXC/C1WBS/logs"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

child_dag_id_program = "dxctechnology_c1_wbs_import_child_program_v1"
child_dag_id_cost_center = "dxctechnology_c1_wbs_import_child_cost_center_v1"
child_dag_id_project = "dxctechnology_c1_wbs_import_child_project_v1"
child_dag_id_client = "dxctechnology_c1_wbs_import_child_client_v1"

execution_timeout_days = 14

disabled = True
