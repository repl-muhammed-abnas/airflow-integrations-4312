region = 'us-east-1'
environment = 'pre-production'

report_name = 'Replicon timesheets list'
time_population_script_name = 'DNT_PopulationScriptextended'
expected_report_columns = 'User Name,Login Name,Timesheet Period,TimesheetPeriodUri'

execution_timeout_days = 14
max_active_runs = 1
max_child_active_runs = 10
schedule_interval = '0 22 * * *'
time_zone = 'America/Chicago'
s3_download_link_expiry = 7*24*60*60
