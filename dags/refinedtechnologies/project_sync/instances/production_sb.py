from refinedtechnologies.project_sync.config import *

# Instance-specific configuration
instance = "production"
environment = 'production'
company_key = 'RefinedTechnologiesIncSandbox'

# Connection IDs
replicon_conn_id = "standard_sf_RefinedTechnologiesIncSandbox_replicon"
salesforce_conn_id = "standard_sf_RefinedTechnologiesIncSandbox_salesforce2"

# Salesforce user ID to exclude from queries (typically the integration user)
salesforce_integration_user_id = f'refinedtechnologies_project_sync_salesforce_integration_user_id_to_exclude_{instance}'

# DAG IDs
master_dag_id = f'{company_key}_project_sync_salesforce_master_{instance}'
process_project_child_dag_id = f'{company_key}_project_sync_salesforce_process_project_child_{instance}'
search_client_replicon_child_dag_id = f'{company_key}_project_sync_salesforce_search_client_child_{instance}'

# Airflow Variable toggle for running the search-client child flow as a single batched task
can_run_batch_task_var_name = f'refinedtechnologies_project_sync_can_run_batch_task_{instance}'

# Airflow Variable holding the last successful Salesforce sync watermark
last_sync_time_variable = 'standard_refinedtechnologiesincsandbox_project_sync_last_sync_time'

# Execution settings
execution_timeout_days = 14
max_active_runs = 1
max_active_runs_child = 5