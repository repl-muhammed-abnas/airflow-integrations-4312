region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
instance = "dxctrial01"

input_filepath = "/DXC/wf39_psa/Input"
c1_filepath = "/DXC/wf39_psa/c1_processing"
compass_filepath = "/DXC/wf39_psa/compass_processing"

archive_filepath = "/DXC/wf39_psa/Archive"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = 30
file_sensor_timeout = 15
execution_timeout_days = 14
