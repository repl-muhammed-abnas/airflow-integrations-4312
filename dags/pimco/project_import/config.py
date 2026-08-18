region = 'us-east-1'
environment = 'pre-production'

master_schedule_interval = 30

max_active_runs_master = 1
child_dag_max_active_runs = 1
max_active_runs = 1
execution_timeout_days = 14
process_project_max_active_run = 5

can_run_batch_task_var_name = "pimco_project_import_can_run_batch_task"
