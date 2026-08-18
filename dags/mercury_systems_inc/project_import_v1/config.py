region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
max_active_runs_second_child = 1
max_active_runs_child = 5
execution_timeout_days = 14
parallel_count = 1
time_zone = "America/New_York"
master_dag_interval = 30

PROJECT_BATCH_COUNT = 1

can_run_batch_task_var_name = 'can_run_batch_task'

program_mapper = ['WO', 'Proj', 'OVERHEADWO', 'OVERHEAD', 'MMIC', 'ITCAP']
dept_mapper = {
    'WO': ['WO', 'ProjWO'],
    'Proj': ['Proj', 'ProjWO'],
    'OVERHEADWO': ['WO', 'ProjWO']
}
