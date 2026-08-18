# Inherit all default configuration
from ipipeline.access_token_generation.config import *

# AWS Configuration
instance = 'uat'
environment = 'pre-production'

# Instance Identification
company_key = "iPipelineSB"

# Connection IDs
replicon_conn_id = 'ipipelinesb_replicon_repliconint.userimport'
http_conn_id_tempo_token_generation = f'ipipeline_tempo_token_generation_http_{instance}'
http_conn_id_jira_token_generation = f'ipipeline_jira_token_generation_http_{instance}'

# Variables
tempo_client_id_secret_var = f"ipipeline_tempo_client_id_secret_variable_{instance}"
jira_client_id_secret_var = f"ipipeline_jira_client_id_secret_variable_{instance}"

tempo_bearer_token_var = f"ipipeline_tempo_bearer_token_{instance}"
tempo_refresh_token_var = f"ipipeline_tempo_refresh_token_{instance}"

jira_bearer_token_var = f"ipipeline_jira_bearer_token_{instance}"

# DAG IDs
tempo_token_generation_dag_id = f"ipipeline_tempo_token_generation_{instance}"
jira_token_generation_dag_id = f"ipipeline_jira_token_generation_{instance}"
