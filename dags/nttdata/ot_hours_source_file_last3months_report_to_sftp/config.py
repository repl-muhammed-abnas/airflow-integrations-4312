region = 'us-east-1'
environment = 'production'
instance = 'production'

company_key = 'nttdata'
eastern_timezone = 'America/New_York'
schedule_interval = '0 9 * * 1'

report_name = "OT Hours Source File__Last 3 Months"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
filepath = "/PITOREPLICON/SAP/"
filepath1 = "/RepliconToBW/BW_PROD/"
