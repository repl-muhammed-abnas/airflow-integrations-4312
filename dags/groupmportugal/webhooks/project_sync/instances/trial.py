# pylint: disable=wildcard-import unused-wildcard-import
from groupmportugal.webhooks.project_sync.config import *

environment = 'pre-production'

instance = "trial"

company_key = "GroupMPortugalafmig"
bearer_token_var = f'groupmportugalafmig_webhooks_{instance}_secret'

replicon_conn_id = "groupmportugalafmig_replicon_admin"

project_sync_webhook_main_dag = f"groupmportugal_project_sync_webhook_{instance}"

project_sync_master_dag_id = f'groupmportugal_project_sync_master_{instance}'
