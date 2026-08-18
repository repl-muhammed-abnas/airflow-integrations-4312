# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.user_import.config import *

environment = 'pre-production'

instance = "dev"

company_key = "AlvarezandMarsalHoldingsDev"
bearer_token_var = 'alvarezandmarsalholdingsdev_user_import_webhook_token'

replicon_conn_id = "alvarezandmarsalholdingsdev_replicon_radmin1"

user_import_webhook_main_dag = f"alvarezandmarsalholdings_user_import_webhook_{instance}"

user_import_master_dag_id = f'alvarezandmarsalholdings_user_import_master_{instance}_v3'

