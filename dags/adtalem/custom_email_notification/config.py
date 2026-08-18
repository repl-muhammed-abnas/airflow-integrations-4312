region = 'us-east-1'
environment = 'pre-production'
company_key = 'adtalemafmig'
replicon_conn_id = "adtalem_replicon_migration"

schedule_interval = 10
client_sftp_conn_id = 'replicon_sftp'

child_max_active_runs = 10

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
