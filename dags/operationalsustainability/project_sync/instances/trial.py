from operationalsustainability.project_sync.config import *

instance = "trial"

region = 'us-east-1'
environment = 'pre-production'
company_key = 'OperationalSustainabilityafmig'

replicon_conn_id = "operationalsustainability_project_sync_trial" 
salesforce_conn_id = "operationalsustainability_salesforce_trial" 

max_active_runs = 1
max_active_child_runs = 1

execution_timeout_days = 1

master_dag_id = f'operationalsustainability_sync_opportunities_from_salesforce_to_replicon_project_master_{instance}'
process_each_opprtunity_child_dag_id = f'process_each_opprtunity_child_dag_{instance}'

base_variable_name = 'operationalsustainability_sync_opportunities_from_salesforce_to_replicon_project'

### Variables configured in Airflow ###
# In Workato, types_to_be_synced = 'ALL'
types_to_be_synced = f'{base_variable_name}_types_to_be_synced_{instance}'

# In Workato, operation = 'OR'
operation = f'{base_variable_name}_operation_{instance}'

# In Workato, sync_opportunities_with_no_types = No
sync_opportunities_with_no_types = f'{base_variable_name}_sync_opportunities_with_no_types_{instance}'

# In Workato, stages_to_be_synced = 'Closed Won'
stages_to_be_synced = f'{base_variable_name}_stages_to_be_synced_{instance}'

# In Workato, probability = 0
probability = f'{base_variable_name}_probability_{instance}'

# In Workato, to_update = No. True or False in Airflow
to_update = f'{base_variable_name}_to_update_flag_{instance}'

last_modified_datetime = f'{base_variable_name}_last_modified_date_{instance}'