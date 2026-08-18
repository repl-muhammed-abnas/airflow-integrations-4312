# pylint: disable=wildcard-import unused-wildcard-import
from addsystems.webhooks.time_entry_sync.config import *

environment = 'production'
instance = "production"
company_key = "ADDSystems"
bearer_token_var = 'addsystems_time_sync_prod_token'

replicon_conn_id = "ADDSystems_replicon_integration_admin"

time_entry_sync_dag_id=f"addsystems_time_sync_master_{instance}_v1"
