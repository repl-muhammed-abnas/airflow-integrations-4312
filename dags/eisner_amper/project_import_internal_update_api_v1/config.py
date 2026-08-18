region = 'us-east-1'
environment = 'pre-production'

max_active_runs_master = 1
max_active_runs_clients = 10
max_active_runs_projects = 10
max_active_runs_tasks = 20
max_active_runs_log_generation = 1

max_active_runs_sort_tasks_master = 1
max_active_runs_sort_tasks_child = 10

execution_timeout_days = 14

default_task_name = 'Default Task'
default_task_code = 'DFT'

timezone = 'Etc/UTC'
master_dag_interval_sort_tasks = "0 * * * *"

TASK_BATCH_COUNT = 2
