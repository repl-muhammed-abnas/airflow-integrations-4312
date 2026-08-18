region = 'us-east-1'
environment = 'production'
instance = 'production'

company_key = '10272kmc'
eastern_timezone = 'America/New_York'
schedule_interval = '0 2 * * 2'

report_name = "Punch Timesheet Details"

tenant_email = "HR@rpmmachinery.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
filepath = "/ReportExtract/"
