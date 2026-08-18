region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

sftp_conn_id = "sftp_useast2"
input_filepath = "/Test/Inbound/CWFPOBalances/Input"
archive_filepath = "/Test/Inbound/CWFPOBalances/Archive"
log_filepath = "/Test/Inbound/CWFPOBalances/Logs"

archive_reference_filepath = "/Test/Inbound/CWFPOBalances/reference/old"
reference_filepath = "/Test/Inbound/CWFPOBalances/reference/"
integration_report_name = "User list for purchase and worker order - Replicon"
key_namespace = "DXC_PurchaseOrderRateTypesBalanceDetails"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = 30
file_sensor_timeout = 15
execution_timeout_days = 14

input_file_size_threshold = 175

max_active_runs_master = 1
max_active_runs_child = 2
