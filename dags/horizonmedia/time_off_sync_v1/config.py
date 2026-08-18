region = 'us-east-2'
environment = 'pre-production'
company_key = 'horizonmediatrial01'
replicon_conn_id = "replicon_horizonmedia_trial"

schedule_interval = 10
client_sftp_conn_id = 'client_horizon_sftp'

max_child_active_runs = 10
max_active_runs = 1

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

execution_timeout_days = 14
