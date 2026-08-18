"""
iPipeline JIRA Time Import - UAT Instance Configuration
"""
# Inherit all default configuration
from ipipeline.time_import.config import *

# AWS Configuration
instance = 'uat'
environment = 'pre-production'

# Instance Identification
company_key = "iPipelineSB"

# Connection IDs
replicon_conn_id = 'ipipelinesb_replicon_repliconint.userimport'

# This HTTP connection only contains the tempo endpoint, bearer token is generated separately using OAUTH (refresh token workflow)
tempo_conn_id = 'ipipeline_tempo_http'

# This HTTP connection only contains the endpoint with CloudID, bearer token is generated separately using OAUTH (client credentials workflow)
jira_conn_id = 'ipipeline_jira_basic_auth'

# Variables
tempo_bearer_token_var = f"ipipeline_tempo_bearer_token_{instance}"
jira_bearer_token_var = f"ipipeline_jira_bearer_token_{instance}"

can_run_batch_task_var_name = f"ipipeline_jira_time_import_{instance}_can_run_batch_task"

tempo_time_entries_lookback_date = f"ipipeline_jira_time_import_tempo_time_entries_lookback_timestamp_{instance}"

# Email configuration
tenant_email = 'jira-replicon-integration@ipipeline.com'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

version = ''  # _v1, _v2
dag_id_suffix = f"{instance}{version}"

# DAG IDs
master_dag_id = f"ipipeline_jira_time_import_master_{dag_id_suffix}"
process_jira_project_info_child_dag_id = f"ipipeline_jira_time_import_process_jira_project_info_child_{dag_id_suffix}"
process_each_user_time_entries_child_dag_id = f"ipipeline_jira_time_import_process_each_user_time_entries_child_{dag_id_suffix}"
process_each_time_entry_child_dag_id = f"ipipeline_jira_time_import_process_each_time_entry_child_{dag_id_suffix}"
process_log_generation_child_dag_id = f"ipipeline_jira_time_import_process_log_generation_child_{dag_id_suffix}"

disabled = True
