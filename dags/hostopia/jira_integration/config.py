region = 'us-east-2'
environment = 'pre-production'

sftp_conn_id = 'sftp_useast2'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

schedule_interval = '30 * * * *'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
http_conn_id = 'jira_connector'
execution_timeout_days = 14
