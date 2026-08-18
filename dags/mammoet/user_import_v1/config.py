region = 'eu-central-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14
trigger_parallel_dagrun_count_process_users = 10
disable_master_dag_active_runs = 1
disable_master_dag_interval = "0 1 * * *"
disable_child_dag_active_runs = 5
sumo_conn_id = 'sumologic-dagrunlogger'
est_timezone = 'America/Los_Angeles'
user_disable_report_name = "***User Disable Template Report ***"


process_payload_max_active_runs = 2
process_users_max_active_run = 16
process_add_user_max_active_runs = 16
process_update_user_max_active_runs = 16
process_log_generation_max_active_runs = 1
process_groups_max_active_runs = 1
process_supervisor_assignment_max_active_runs = 16
