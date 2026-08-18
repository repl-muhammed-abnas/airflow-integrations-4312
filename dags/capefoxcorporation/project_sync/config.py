# Common configuration for Capefox Corporation Project Sync DAGs
region = 'us-east-1'
environment = 'pre-production'

# DAG Configuration
max_active_runs = 1
execution_timeout_days = 14
child_dag_max_active_runs = 5
date_time_format = "%Y-%m-%dT%H:%M:%S"
dag_max_active_tasks = 10000
master_dag_interval = 5
time_zone = 'Etc/UTC'
costpoint_time_zone = 'US/Eastern'
schedule_interval = "*/1 * * * *"
download_link_expiration_seconds = 7*24*60*60  # 7 days

# Permission Configuration
project_manager_permission_name = 'Project Manager'

# Deltek Costpoint Configuration
deltek_costpoint_company_ids = ['1', '2', '3', '4']

# Custom Field Names
proj_purchase_order_no = 'Purchase Order No'
proj_opportunity_id = 'Opportunity ID'
proj_project_classification = 'Project Classification'
proj_user_company = 'Company'

# Log Generation Configuration
log_generation_dag_interval = '0 * * * *'
lookup_log_timestamp_hours = 1
child_dag_log_generation_max_active_runs = 20
trigger_parallel_dagrun_count = 10
