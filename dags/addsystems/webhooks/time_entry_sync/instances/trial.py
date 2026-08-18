# pylint: disable=wildcard-import unused-wildcard-import
from addsystems.webhooks.time_entry_sync.config import *

environment = 'pre-production'
instance = "trial"
company_key = "addsystemstrial01"
bearer_token_var = 'addsystems_time_sync_trial_token'

replicon_conn_id = "ADDSystemsblanktrial_replicon_admin"

time_entry_sync_dag_id=f"addsystems_time_sync_master_{instance}_v1"
