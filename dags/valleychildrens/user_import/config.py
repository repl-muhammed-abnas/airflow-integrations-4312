region = "us-east-1"
environment = "pre-production"
execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

max_active_runs_master = 1
max_active_runs_process_add_user = 4
max_active_runs_process_user_update = 4
max_active_runs_process_supervisor_assignment = 4
max_active_runs_process_update_user_time_off_assign = 4
max_active_runs_process_rehire_update_user_time_off_assign = 4
max_active_runs_process_timeoff_add_new_user = 4
max_active_runs_process_time_off_policy_add_pto = 4
max_active_runs_process_time_off_policy_update_on_fte_change = 4
max_active_runs_process_timeoff_policy_payoutbalance = 4
max_active_runs_process_log_generation = 1

trigger_parallel_dagrun_count_process_users = 15

ACTIVE_STATUS = ['Active', 'Paid Leave']
DISABLE_STATUS = ['Terminated', 'Suspended', 'Retired']
BATCH_COUNT = 3

pacific_timezone = 'America/Los_Angeles'
sumo_conn_id = 'sumologic-dagrunlogger'
report_name = 'User list - For Integration'
