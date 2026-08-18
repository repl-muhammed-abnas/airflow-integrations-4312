from operationalsustainability.client_sync.config import *

instance = 'trial'
environment = 'production'

company_key = 'OperationalSustainabilitytrial'
replicon_conn_id = 'OperationalSustainabilityafmig-replicon-apiuser'

qbo_conn_id = 'OperationalSustainabilityafmig-quickbooks-apiuser'

master_dag_id = f'operationalsustainability_client_export_master_{instance}'
child_dag_id = f'operationalsustainability_client_export_add_client_child_{instance}'


can_run_batch_task = f'OperationalSustainability_Clientsync_{instance}_can_run_batch_task'
last_sync_time_variable = 'standard_OperationalSustainability_Clientsync_last_sync_time'

