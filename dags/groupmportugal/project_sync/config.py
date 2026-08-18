region = 'eu-central-1'
environment = 'pre-production'
max_active_runs_master = 1
max_active_runs_child = 5
execution_timeout_days = 14

log_generation_dag_interval = "0 */2 * * *"
lookup_log_timestamp_hours:int = 2

parallel_count_clients= 3
parallel_count= 3
