region = "us-east-1"
environment = "pre-production"

# Master DAG configuration
max_active_runs_master = 1
schedule_interval = "0 0 * * *"
time_zone = "America/New_York"

# Child DAG configuration
max_active_runs_child = 5

# Execution timeout for child DAGs (in days)
execution_timeout_days = 14

# Replicon configuration
replicon_base_url = "https://na4.replicon.com/dkpierceassociates"

# Query limits
salesforce_account_query_limit = 100
salesforce_client_manager_query_limit = 200
