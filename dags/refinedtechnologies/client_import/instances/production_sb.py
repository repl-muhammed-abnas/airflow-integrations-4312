from refinedtechnologies.client_import.config import *

instance = "production"

environment = 'production'

company_key = 'RefinedTechnologiesIncSandbox'

replicon_conn_id = "standard_sf_RefinedTechnologiesIncSandbox_replicon"
salesforce_conn_id = "standard_sf_RefinedTechnologiesIncSandbox_salesforce2"

# DAG IDs
master_dag_id = f'{company_key}_client_import_salesforce_master_{instance}'
process_client_child_dag_id = f'{company_key}_client_import_salesforce_process_client_child_{instance}'

# Airflow Variable toggle for running the child flow as a single batched task
can_run_batch_task_var_name = f'refinedtechnologies_client_import_can_run_batch_task_{instance}'

# Airflow Variable holding the last successful Salesforce sync watermark
last_sync_time_variable = 'standard_refinedtechnologiesincsandbox_client_import_last_sync_time'
thread_pool_size_write_csv = 10

# Override execution timeout if needed
execution_timeout_days = 14

