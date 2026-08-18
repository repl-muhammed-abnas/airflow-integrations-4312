region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
gather_project_logs_timeout_hours = 2

master_dag_max_active_runs = 1
trigger_parallel_dagrun_count_process_clients = 2
trigger_parallel_dagrun_count_process_projects = 2
max_active_runs_process_clients = 5
max_active_runs_process_projects = 5
max_active_runs_process_log_generation = 1

dagrun_log_conn_id = 'sumologic-dagrunlogger'
