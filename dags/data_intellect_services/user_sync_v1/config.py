region = 'eu-central-1'
environment = "pre-production"
master_max_active_run = 1
create_new_users_child_dag_max_active_runs = 1
update_users_child_dag_max_active_runs = 1
process_users_child_dag_max_active_runs = 1
sumo_conn_id = 'sumologic-dagrunlogger'

log_generation_dag_interval = "0 11 * * *"
schedule_interval = "30 * * * *"

lookup_log_timestamp_hours = 24
execution_timeout_days = 14

time_zone = "Etc/UTC"
