region = 'eu-central-1'
environment = 'pre-production'

company_key = "itvdaytimetrial01"

sftp_conn_id = "sftp_useast2"  # "sftp-itvdaytime-internal"

replicon_conn_id = "replicon-itvdaytime-radmin"

master_schedule_interval = 30
delimiter = ","
execution_timeout_days = 14

# to be updated as per spec while deploying for UAT
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'
