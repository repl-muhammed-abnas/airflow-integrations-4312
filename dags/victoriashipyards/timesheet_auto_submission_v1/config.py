region = 'us-east-1'
environment = 'pre-production'

company_key = 'VSLSandbox'

max_active_runs = 1
recalculate_max_active_runs = 5

timesheet_report = '***Timesheet Report***'
timesheet_report_to_reopen = '***Timesheet Audit Report For Reopen Action***'

time_zone = 'PST8PDT'

run_report_wait_timeout = 60 * 60 * 24
execution_timeout_days = 14

timesheet_submission_mapper_var = 'victoriashipyards_timesheet_submission_warning_messages_mapper'

schedule_interval = "0 2,17 * * *"
schedule_interval_6_35_am = "35 6 * * *"
schedule_interval_shift_change = "0 */3 * * *"
