region = 'us-east-1'
environment = 'pre-production'

child_dag_max_active_runs = 16
max_active_runs_child = 20
max_active_dag_runs = 1
max_active_runs_process_log_generation = 1

execution_timeout_days = 14
schedule_interval = "0 22 31 3 *"

gather_logs_timeout_hours = 2