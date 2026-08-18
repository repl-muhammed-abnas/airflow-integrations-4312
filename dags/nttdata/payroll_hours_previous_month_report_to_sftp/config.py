region = 'us-east-1'
environment = 'production'
instance = 'production'

company_key = 'nttdata'
eastern_timezone = 'America/New_York'
schedule_interval = '0 9 * * 0'

report_name = "Payroll_Hours_Previous_Month"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
filepath = "/payrollreports/previousmonth/"
