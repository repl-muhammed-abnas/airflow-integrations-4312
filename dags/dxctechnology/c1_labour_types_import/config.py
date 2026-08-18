region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = "repliconsftp"
input_filepath = "/DXC/C1LabourTypes/input"
archive_filepath = "/DXC/C1LabourTypes/archive"
log_filepath = "/DXC/C1LabourTypes/logs"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = 30
file_sensor_timeout = 10

child_dag_create_billing_rate_max_active_runs = 20
child_dag_process_wbs_max_active_runs = 20
compass_child_dag_process_wbs_max_active_runs = 20

execution_timeout_days = 14
