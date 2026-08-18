# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.webhooks_endpoint.project_import_api_webhook.config import *

instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'pwc'
replicon_conn_id = 'pwc-replicon-eu.projectimport'

master_dag_max_active_runs=20

webhook_secret = f'pwc_project_import_webhook_{instance}_secret'

can_redirect_to_workato_var_name = f'pwc_project_import_webhook_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_project_import_webhook_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_project_import_webhook_{instance}_workato_api_token'
project_import_api_process_payload_child_dag_id= f"pwc_project_client_process_payload_master_{instance}_v7"
