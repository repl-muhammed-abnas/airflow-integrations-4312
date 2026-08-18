region = 'eu-central-1'
environment = "pre-production"

execution_timeout_days = 14

trigger_parallel_dagrun_count_process_users = 10

sumo_conn_id = 'sumologic-dagrunlogger'
est_timezone = 'America/Los_Angeles'


master_max_active_run = 2
process_payload_max_active_runs = 2
process_users_max_active_run = 3
process_add_user_max_active_runs = 3
process_update_user_max_active_runs = 3
process_log_generation_max_active_runs = 1
process_groups_max_active_runs = 1
process_supervisor_assignment_max_active_runs = 3
process_multiple_users_dag_max_active_run= 1
