
region = "us-east-2"
environment = "pre-production"

execution_timeout_days = 14

schedule_interval = "0 1 * * *"
user_timesheet_deletion_schedule = "0 6 * * *"

user_timesheet_deletion_report_name = "***User Timesheet Deletion Report***"
report_name = "***User Template - For Non Contractors"
expected_report_columns = "User Name,User Email,ClosureDate,Timesheet Start Date,Timesheet End Date,Timesheet Period,TimesheetPeriodUri,remove_timesheets,User End Date"

process_disable_user_dag_count = 4
max_active_run_master = 1
parallel_dag_run_count = 4
process_time_off_accrual_max_active_runs = 5

allowed_countries = ["ALL"]
