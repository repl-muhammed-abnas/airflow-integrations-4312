# Common configuration for Capefox Corporation Timesheet Sync DAGs

# Environment Configuration
region = 'us-east-1'
environment = 'pre-production'
time_zone = 'US/Eastern'

# DAG Configuration
execution_timeout_days = 14
child_dag_max_active_runs = 2
dag_max_active_tasks = 10000

# Costpoint Line Type Configuration
line_type = 'A'
mo_line_type = 'M'
regular_pay_type = 'REG'

# Custom Field Names
activity_type = 'Activity Type'
work_center = 'Work Center'
proj_mo_project_flag = 'Is MO Project ?'
proj_reference_project_id = 'Build Project'

# Log Generation Configuration
log_generation_dag_interval = '0 * * * *'
lookup_log_timestamp_hours = 1
download_link_expiration_seconds = 7 * 24 * 60 * 60  # 7 days
