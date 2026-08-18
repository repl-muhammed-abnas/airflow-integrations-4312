region = 'us-east-1'
environment = 'pre-production'

time_zone = 'US/Alaska'

max_active_runs_master = 1
max_active_runs_process_logs = 1
max_active_runs_submission_child = 1
max_active_runs_child = 5

trigger_process_timesheet_for_distribution_parallel_count = 3

execution_timeout_days = 14
gather_user_logs_timeout_hours = 5

time_population_script_name = 'Allocate Timesheet Hours'
report_name = 'User Timesheet Details'

master_dag_schedule = '0 5 1,16 * *'

DATE_FORMAT = "%m/%d/%Y"

expected_report_columns = "User Name,Employee ID,Timesheet Period,TimesheetPeriodUri"
