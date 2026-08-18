region = 'us-east-1'
environment = "pre-production"

# File naming configuration
file_prefix_map = {
    'Unisysdev': 'Dev',
    'UnisysUAT': 'UAT',
    'unisyscorporation': 'PROD'
}

user_base_report_name = '***Active Users List***'
project_base_report_name = '***Active Projects List***'

# Schedule configuration
schedule_interval = "30 11 * * *"  # Daily at 11:30 AM IST
timezone = "Asia/Kolkata"

# DAG execution configuration
max_active_runs = 1
max_active_runs_child = 5
execution_timeout_days = 14
parallel_child_dag_count = 5
