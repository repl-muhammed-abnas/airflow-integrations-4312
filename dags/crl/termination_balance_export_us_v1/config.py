region = 'us-east-1'
environment = 'pre-production'

child_dag_max_active_runs = 16
max_active_dag_runs = 1
execution_timeout_days = 14
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
user_report_name = "user details - termination balance usa"
termination_balance_report_name = "Termination balance report usa"

encrypt_output_file = True
file_name_prefix = 'PQ0476'
date_time_format = "%m/%d/%Y, %H:%M:%S"

max_active_runs = 1
execution_timeout_days = 14

time_zone = "America/New_York"

schedule_interval = "0 8,9 * * *"

schedule_interval_daily = "0 7 * * *"

thread_pool_size_write_csv = 50

user_column = "User Name,Location (Current),UserUri,User Start Date,User End Date,Term Exported"

termination_column = "User Name,Time Off Type,Time Off Balance,Employee ID,User Start Date,User End Date,useruri,Employee Status,Location (Current) (Full Path),Home Location"
