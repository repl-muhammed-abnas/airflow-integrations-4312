region = 'eu-central-1'
environment = 'pre-production'

max_active_runs_master = 1
max_active_process_timeoff_child = 5
max_active_runs_process_timeoff_entry = 1
execution_timeout_days = 14
child_wait_execution_timeout = 14
parallel_dagrun_count_process_distict_projects = 5
gather_timeoff_logs_timeout_hours = 2

master_dag_interval = "0 6 * * *"
