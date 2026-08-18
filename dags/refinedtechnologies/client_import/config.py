region = "us-east-1"
environment = "pre-production"

# Master DAG configuration
max_active_runs_master = 1
schedule_interval = "0 * * * *"
time_zone = "America/New_York"

# Child DAG configuration
max_active_runs_child = 5  # Allow up to 10 child DAGs to run in parallel

export_report_name="Refined Technologies Inc Client Import"

thread_pool_size_write_csv = 10

limit = 100
lastModifiedById = '00536000002915qAAA'

# Execution timeout for child DAGs (in days)
execution_timeout_days = 14
