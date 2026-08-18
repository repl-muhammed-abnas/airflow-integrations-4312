region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
instance = "dxctrial01"

sftp_conn_id = "integration_ap"
input_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT1/Input/"
attribute1_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT1/Input/"
attribute2_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/NT1/Input/"

archive_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT1/Archive/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = 30
file_sensor_timeout = 15
execution_timeout_days = 14
