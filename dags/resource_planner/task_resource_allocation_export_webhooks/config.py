# Shared configuration for webhook-driven task resource allocation processing DAGs

region = 'us-east-1'
environment = 'pre-production'

# GraphQL configuration
graphql_endpoint = '/graphql'

# RP Backend API configuration
rp_api_target_table = None  # None = use production table, set to "dummy_rp_source" for testing

# DAG concurrency
new_max_active_runs = 5
deleted_max_active_runs = 5

# Modified event child routing
modified_child_count = 3
modified_child_max_active_runs = 1  # serialize per partition

# DAG configuration defaults
schedule_interval = None
