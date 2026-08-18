# Import common configurations
from capefoxcorporation.project_sync.config import *

# Instance-specific configuration
region = 'us-east-1'
environment = 'pre-production'
instance = 'trial'
company_key = 'capefoxcorporationsb'
replicon_conn_id = 'capefoxcorporationsb_replicon_integration'
deltek_costpoint_conn_id = 'capefoxcorporationsb_deltek_costpoint_32764'
last_run_date_var_name = f'capefoxcorporation_deltek_costpoint_project_sync_last_run_date_{instance}'
get_data_in_chunk_var_name = f'capefoxcorporation_deltek_costpoint_project_sync_get_data_in_chunk_{instance}'
can_run_batch_task_var_name = f'capefoxcorporation_deltek_costpoint_project_sync_run_batch_task_{instance}'
lookup_log_timestamp_var = f'capefoxcorporation_deltek_costpoint_project_sync_lookup_log_timestamp_{instance}'

# Instance-specific email configuration (override common config)
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

master_dag_id = f'capefoxcorporation_deltek_costpoint_project_sync_master_{instance}'
process_each_root_project_child_dag_id = f'capefoxcorporation_deltek_costpoint_project_sync_process_each_root_project_child_{instance}'
log_generation_master_dag_id = f'capefoxcorporation_deltek_costpoint_project_sync_log_generation_{instance}'

disabled = True
