# UAT Instance Configuration for iPipeline Salesforce Integration
from ipipeline.project_client_import.config import *

# AWS Configuration
region = 'us-east-1'
environment = 'pre-production'

# Instance Identification
instance = "uat"
company_key = "iPipelineSB"

# Connection IDs
replicon_conn_id = 'ipipelinesb_replicon_repliconint.userimport'
salesforce_conn_id = "ipipeline_salesforce_sandbox"

version = "" # _v1, _v2, etc.

dag_id_suffix = f"{instance}{version}"

master_dag_id = f"ipipeline_project_client_import_master_{dag_id_suffix}"
process_client_child_dag_id = f"ipipeline_project_client_import_process_client_child_{dag_id_suffix}"
process_project_child_dag_id = f"ipipeline_project_client_import_process_project_child_{dag_id_suffix}"
process_log_generation_dag_id = f"ipipeline_project_client_import_process_logs_{dag_id_suffix}"

tenant_email = 'salesforce-replicon-integration@ipipeline.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

accounts_lookback_period_start_timestamp = f"ipipeline_project_client_import_accounts_lookback_start_timestamp_{instance}"
opportunities_lookback_period_start_timestamp = f"ipipeline_project_client_import_opportunities_lookback_start_timestamp_{instance}"
bypass_trial_instance_check = f"ipipeline_project_client_import_bypass_trial_instance_check_{instance}"

can_run_batch_task_var_name = f"ipipeline_project_client_import_{instance}_can_run_batch_task"
