region = 'us-east-1'
environment = 'pre-production'

schedule_interval='0 1 * * 1-5'
timezone= "America/New_York"

max_active_runs = 1
duration_days = 28
execution_timeout_days = 15
child_dag_max_active_runs = 10
max_parallel_run = 25

report_name = "Missing Time Entry"
timesheet_report_name = "Missing Time Entry with Timesheet"
timesheet_report_columns = "Timesheet Period,useruri,Entry Date"
report_columns = "User First Name,User Last Name,User Email,Date,Scheduled Work Hours,Time Off Hours,Total Actual Hours,User Status,useruri"
