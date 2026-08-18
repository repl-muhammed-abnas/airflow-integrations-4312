instance = 'PwC'
region = 'eu-central-1'
environment = 'production'

company_key = 'PwC'
username = "Australia, Admin"

max_active_runs = 1
location_name = 'Australia'

replicon_conn_id = 'Pwc-replicon-admin.australia'

report_name = 'Time Data Export from Replicon to iPower - New'

sftp_conn_id = 'pwcglobal-AUS-MFT-PRD-replicon'
output_filepath = '/PwCGBL_Replicon_PRD/Australia/Inbound/Timesheet ingest/'

tenant_email = "au_repliconadmin@pwc.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

time_zone = 'Australia/Sydney'

schedule_interval = "30 0 * * *"
