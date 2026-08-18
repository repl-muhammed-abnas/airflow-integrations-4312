# Common configuration for Refined Technologies Project Sync integration
region = "us-east-1"
environment = "pre-production"

# DAG execution settings
max_active_runs = 1
schedule_interval = "15 15 * * 1"  # Every Monday at 3:15 PM ET
time_zone = "America/New_York"

# Sumo Logic connection IDs
sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

# Execution timeout for DAG runs (in days)
execution_timeout_days = 14

# Salesforce query limits
salesforce_query_limit = 200  # Maximum records per query
