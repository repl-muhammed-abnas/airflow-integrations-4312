region = "us-east-1"

environment = "pre-production"

execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

max_active_runs_process_non_live_location = 1
max_active_runs_process_groups = 1
max_active_runs_process_users = 10
max_active_runs_process_new_users = 20
max_active_runs_process_update_users = 20
max_active_runs_process_log_generation = 1

max_active_runs_process_timeoff_type_no_accrual = 10

trigger_parallel_dagrun_count_process_users = 10

ACTIVE_STATUS = ['Active','Paid Leave','Furlough','Dormant']
DISABLE_STATUS = ['Terminated','Unpaid Leave','Suspended','Retired','Discarted','Deceased']
