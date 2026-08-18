region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntFtp'
sftp_conn_id = 'repliconsftp'

max_active_child_dag_runs = 10
execution_timeout_days = 14

process_wbs_child_parallel_dagruns_count = 20

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
