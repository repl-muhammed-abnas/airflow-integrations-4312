region = 'us-east-1'
environment = 'production'
instance = 'production'

company_key = 'nttdata'
central_timezone = 'America/Chicago'
schedule_interval = '0 8 * * *'

report_name = "PTO Acorio"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
filepath = "/PITOREPLICON/ACORIO/"
