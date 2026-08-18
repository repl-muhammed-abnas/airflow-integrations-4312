region = 'eu-central-1'
environment = 'pre-production'
company_key = 'PwCinternal'
replicon_conn_id = 'replicon_pwcglobal'

# pylint: disable=line-too-long
validation_message = "Time entries on future timesheet periods can be entered but not submitted. Timesheets that contain only ‘absences’ will then be auto-submitted at the end of the timesheet period"
dag_max_active_runs = 3
dag_max_active_tasks = 128
recalculate_max_active_runs = 5
execution_timeout_days = 14
run_report_wait_timeout = 60 * 60 * 24
zero_hours_schedule_interval = '0 21 * * *'
timeoff_schedule_interval = '30 21 * * *'
time_zone = 'UTC'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

execution_timeout_mins_write_csv = 90
thread_pool_size_count = 50
