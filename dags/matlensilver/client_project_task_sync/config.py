region = 'us-east-2'
environment = 'pre-production'
company_key = 'repliconmatlentrial01'
replicon_conn_id = 'repliconmatlentrial01_replicon_admin'
sftp_conn_id = 'sftp_useast2'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_interval = 30
max_active_runs_clients = 10
max_active_runs_projects = 10
max_active_runs_tasks = 10
