region = 'eu-central-1'
environment = 'pre-production'
company_key = 'pwcinternal'

max_active_runs = 1
location = ''
schedule_interval = None
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

sftp_conn_id = 'integartion-ftp'
output_filepath = '/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/WD/'
log_filepath = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeQA/'
alternate_log_path = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/WD/_logs/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

time_zone = 'Europe/Paris'
