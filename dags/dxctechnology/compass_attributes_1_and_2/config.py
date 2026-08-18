region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-RepliconIntCOMPASS'
sftp_conn_id = 'sftp_dxc_compass_attr1_attr2'

input_filepath_attr1 = '/Production/Inbound/COMPASSAttributes1&2/Attribute1'
input_filepath_attr2 = '/Production/Inbound/COMPASSAttributes1&2/Attribute2'
archive_filepath = '/Production/Archive/COMPASSAttributes1&2/'
log_filepath = '/Production/Logs/COMPASSAttributes1&2/'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_max_active_runs = 1
child_dag_max_active_runs = 128
execution_timeout_days = 14
post_batch_size = 1000

wbs_skiplist = ("ES1-ESSI4.03.01", "IN1-ESSI3.03.01", "AU1-ESSI3.03.01", "NL1-ESSI3.03.01",
                "US1-SECV0.01.19", "DE1-ESSI3.03.01", "GB1-ESSI3.03.01", "GB1-ESSI3.03.01")
disabled = True
