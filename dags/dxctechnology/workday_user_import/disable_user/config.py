
region = "us-east-2"
environment = "pre-production"

execution_timeout_days = 14

schedule_interval = "0 1 * * *"

report_name = "***User Template - For Non Contractors"

process_disable_user_dag_count = 4
max_active_run_master = 1
parallel_dag_run_count = 4
process_time_off_accrual_max_active_runs = 5
