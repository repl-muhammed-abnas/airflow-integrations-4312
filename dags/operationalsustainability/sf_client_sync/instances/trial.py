from operationalsustainability.sf_client_sync.config import *

instance = 'trial'
environment = 'production'

company_key = 'OperationalSustainabilitytrial'
replicon_conn_id = 'standard_sf_OperationalSustainabilityafmig_replicon'

sf_conn_id = 'standard_sf_OperationalSustainabilityafmig_salesforce2'

master_dag_id = f'operationalsustainability_client_import_from_sf_master_{instance}'
child_dag_id = f"operationalsustainability_client_import_from_sf_process_each_account_{instance}"

last_sync_time_variable = 'standard_OperationalSustainability_Client_Import_from_sf_last_sync_time'

created_date_format = "%Y-%m-%dT%H:%M:%S.%f%z"
