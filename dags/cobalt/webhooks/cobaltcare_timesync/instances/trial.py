# pylint: disable=wildcard-import unused-wildcard-import
from cobalt.webhooks.cobaltcare_timesync.config import *

environment = 'pre-production'

instance = "trial"

company_key = "Cobaltafmig"
bearer_token_var = 'cobalafmig_time_sync_webhook_token'

replicon_conn_id = "cobalafmig_replicon_casey.robinson"

time_sync_webhook_main_dag = f"cobalt_time_sync_webhook_{instance}"

time_sync_master_dag_id = f"cobaltcare_zendesk_to_replicon_timesync_master_{instance}"

disabled=True
