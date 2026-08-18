region = 'us-east-1'
environment = 'pre-production'

child_dag_max_active_runs = 5
max_active_dag_runs = 1
execution_timeout_days = 14
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
reg_to_temp_balance_report_name = "Regular to Temp Export USA"

encrypt_output_file = True
file_name_prefix = 'PQ0476'
date_time_format = "%m/%d/%Y, %H:%M:%S"

max_active_runs = 1
execution_timeout_days = 14

time_zone = "America/New_York"

schedule_interval_daily = "0 7 * * *"

schedule_interval = "0 8,9 * * *"

thread_pool_size_write_csv = 50

reg_to_temp_column = "User Name,Time Off Type,Time Off Balance,Employee ID,User Start Date,User End Date,useruri"
