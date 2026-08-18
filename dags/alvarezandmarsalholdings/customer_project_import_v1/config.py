environment = 'pre-production'
region = 'us-east-1'

master_schedule_interval = 30

max_active_runs_master = 1

execution_timeout_days = 14
gather_project_logs_timeout_hours = 2

PROJECT_BATCH_COUNT = 5

master_dag_max_active_runs = 1
trigger_parallel_dagrun_count_process_clients = 2
trigger_parallel_dagrun_count_process_projects = 2
max_active_runs_process_clients = 5
max_active_runs_process_projects = 10
max_active_runs_process_tasks = 5
max_active_runs_process_log_generation = 1

# Project OEF
PROJECT_PROFILE = "Project Profile"
CONTROLLING_AREA = "Controlling Area"
FEDERAL_PROJECT = "Federal Project"

# Task OEF
BILLING_RESPONSIBLE = "Billing Responsible"
BILLING_CONTROL_CATEGORY = 'Billing Control Category'
WORK_PACKAGE_CODE = 'work_package_code'

# Permissions
PERMISSIONS = [
    "Project Manager",
    "End User with Report Edit",
    "Supervisor"
]

dagrun_log_conn_id = 'sumologic-dagrunlogger'
