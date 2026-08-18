from dkpierceassociates.client_sync.config import *

instance = "prod"

environment = 'production'

company_key = 'dkpierceassociatesafmig'

replicon_conn_id = "replicon_dkpierceassociatesafmig_admin"
salesforce_conn_id = "standard_sf_dkpierceassociates_salesforce2"

# DAG IDs
master_dag_id = f'dkpierceassociates_client_sync_master_{instance}'
process_account_dag_id = f'dkpierceassociates_client_sync_child_{instance}'

# Override execution timeout if needed
execution_timeout_days = 14

#included for exclusion of entries related to mentioned userId
salesforce_integration_user_id = "00540000002dqMkAAI"

last_sync_time_variable = 'standard_dkpierceassociates_clientSync_last_sync_time'
