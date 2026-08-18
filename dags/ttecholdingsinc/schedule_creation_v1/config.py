region = 'us-east-1'
environment = 'pre-production'

schedule_interval = "0 0 * * *"
time_zone = "America/Chicago"

execution_timeout_days = 14
master_dag_active_runs = 1
child_dag_active_runs = 2
master_dag_interval = 30

default_shift_name = '09001700'
user_report_name = '***Active Users List***'
expected_report_columns = 'Employee ID,UserUri,User Status,User Start Date,User End Date,Schedule Name (Current)'
