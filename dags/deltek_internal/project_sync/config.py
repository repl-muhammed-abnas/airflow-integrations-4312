region = 'us-east-1'
environment = "pre-production"
# DAG execution settings
execution_timeout_days = 14
max_active_run_master = 1
max_active_run_child = 10
batch_size = 1  # Process one opportunity at a time for detailed error tracking
parallel_dag_run_count = 5  # Process up to 5 opportunities in parallel
schedule_interval = "0 0 * * *"
# Salesforce query settings
# Only fetch opportunities with 50% probability (Best Case) and Professional Services growth type
growth_type_filter = 'Professional Services'
probability_filter = 'Best Case - 50%'
# Workflow name for sync tracking
workflow_name = 'salesforce_to_polaris_project_sync'
provider = 'salesforce'
query_limit = 100
project_template = 'Enterprise Implementation template'
project_modification_save_uri = 'urn:replicon:project-modification-option:save'