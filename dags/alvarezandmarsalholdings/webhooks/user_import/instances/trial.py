from alvarezandmarsalholdings.webhooks.user_import.config import *

environment = 'pre-production'

instance = "trial"

company_key = "AlvarezandMarsalHoldingsDevtrial01"
bearer_token_var = 'alvarezandmarsalholdingsdevtrial01_user_import_webhook_token'

replicon_conn_id = "alvarezandmarsalholdingsdevtrial01_replicon_radmin1"

user_import_webhook_main_dag = f"alvarezandmarsalholdings_user_import_webhook_{instance}"

user_import_master_dag_id = f'alvarezandmarsalholdings_user_import_master_{instance}_v2'
disabled=True
