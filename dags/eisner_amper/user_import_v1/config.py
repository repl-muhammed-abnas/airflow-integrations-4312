region = 'us-east-1'
environment = 'pre-production'

master_dag_interval = 30
max_active_runs_master = 1
execution_timeout_days = 14
max_active_runs_child = 5
child_dag_max_active_runs = 2
master_dag_max_active_runs = 1

time_zone = 'America/New_York'

BATCH_COUNT = 3

max_active_parallel_runs_child = 5
