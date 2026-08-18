region = 'us-east-1'
environment = 'production'
instance = 'production'

company_key = 'nttdata'
eastern_timezone = 'America/New_York'
schedule_interval = '0 7 * * *'

report_name = "**NTTData New TimeOff Extract- 2020"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
filepath = "/PITOCLARITY/PROD/PTO/"
