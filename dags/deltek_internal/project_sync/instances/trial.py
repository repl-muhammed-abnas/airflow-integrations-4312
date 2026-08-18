
from deltek_internal.project_sync.config import *

instance = 'trial'
environment = 'pre-production'

company_key = "RepliconPinctrial01"

replicon_conn_id = 'standard_sf_RepliconPinctrial01_trial_replicon_repliconint'
salesforce_conn_id = 'standard_sf_RepliconPinctrial01_trialafmig_salesforce2'

master_dag_id = f"RepliconPinctrial01_project_sync_master_{instance}"

last_sync_time_variable = 'standard_internal_implementation_Project_Import_from_sf_last_sync_time'

created_date_format = "%Y-%m-%dT%H:%M:%S.%f%z"