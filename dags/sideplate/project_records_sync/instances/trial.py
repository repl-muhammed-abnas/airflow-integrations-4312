from sideplate.project_records_sync.config import *

instance = "trial"

environment = 'pre-production'

company_key = 'sideplateafmig'

replicon_conn_id = "standard_replicon_sideplateafmig_project_records_sync_admin"
salesforce_conn_id = "standard_sf_sideplateafmig_project_records_sync_salesforce_admin"

# DAG IDs
master_dag_id = f'sideplateafmig_project_records_sync_master_{instance}'
process_opportunity_dag_id = f'sideplateafmig_project_records_sync_child_{instance}'
updateprojectoef_sideplate_dag_id = f'sideplateafmig_project_records_sync_update_project_{instance}'
# Override execution timeout if needed
execution_timeout_days = 14
salesforce_integration_user_id = "00540000002dqMkAAI"

last_sync_time_variable = 'standard_sideplateafmig_project_records_sync_last_sync_time'
