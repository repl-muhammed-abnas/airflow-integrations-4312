environment = 'pre-production'
region = 'us-east-2'

master_schedule_interval = 30
execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child = 10
max_active_runs_batch_child = 1

timesheet_report_name = "Time sheet report"


tenant_email = '{{ var.value.dagrun_internal_log_email }}'


internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
