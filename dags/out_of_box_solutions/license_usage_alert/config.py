region = 'us-east-1'
environment = 'pre-production'

sftp_conn_id = 'sftp_useast2'
internal_logs_email =  '{{ var.value.dagrun_internal_testing_email }}'
# runs at every 10am EST converted to UTC 2pm
schedule_interval = '0 14 * * *'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
