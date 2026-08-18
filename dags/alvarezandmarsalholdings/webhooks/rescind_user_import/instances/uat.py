# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.rescind_user_import.config import *

environment = 'pre-production'

instance = "uat"

company_key = "AlvarezandMarsalHoldingsUAT"
bearer_token_var = 'alvarezandmarsalholdingsuat_rescind_user_import_webhook_token'

replicon_conn_id = "alvarezandmarsalholdingsuat_replicon_radmin1"

rescind_user_import_master_dag_id = f"alvarezandmarsalholdings_rescind_user_import_master_{instance}"

rescind_user_import_webhook_main_dag = f"alvarezandmarsalholdings_rescind_user_import_webhook_{instance}"
