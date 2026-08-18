region = "us-east-1"
environment = 'pre-production'

# Master DAG configuration
max_active_runs_master = 5
schedule_interval = "0 0 * * *"
time_zone = "America/New_York"

# Child DAG configuration
max_active_runs_child = 5
max_active_runs_sub_child = 5
parallel_dag_run_count = 5
# Execution timeout for child DAGs (in days)
execution_timeout_days = 14
parallel_trigger_dagrun_count = 5
