# pylint: disable=wildcard-import unused-wildcard-import
from groupmportugal.webhooks.project_sync.config import *

environment = 'production'

instance = "prod"

company_key = "GroupMPortugal"
bearer_token_var = 'groupmportugal_project_sync_webhook_token'

replicon_conn_id = "groupmportugal_replicon_admin"

project_sync_webhook_main_dag = f"groupmportugal_project_sync_webhook_{instance}"

project_sync_master_dag_id = f'groupmportugal_project_sync_master_{instance}'
