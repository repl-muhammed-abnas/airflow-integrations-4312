region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

sftp_conn_id = "Airflowmig_useast2"
input_filepath = "/Test/Inbound/C1TaskandLabortypes/TaskInput"
archive_filepath = "/Test/Inbound/C1TaskandLabortypes/Archive"
log_filepath = "/Test/Inbound/C1TaskandLabortypes/TaskInputLogs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = 30
file_sensor_timeout = 15
execution_timeout_days = 14

child_dag_process_wbs_max_active_runs = 10
child_dag_update_task_max_active_runs = 10
child_dag_create_task_max_active_runs = 10
c1_task_import_master_max_active_runs = 1
