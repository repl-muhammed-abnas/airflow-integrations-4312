# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.rescind_user_import.config import *

environment = 'pre-production'

instance = "sandbox"

company_key = "alvarezandmarsalsb"

replicon_conn_id = "alvarezandmarsalsb_replicon_repliconint.userimport"

bearer_token_var = 'alvarezandmarsalsb_rescind_user_import_webhook_token'

rescind_user_import_master_dag_id = f"alvarezandmarsalholdings_rescind_user_import_master_{instance}"

rescind_user_import_webhook_main_dag = f"alvarezandmarsalholdings_rescind_user_import_webhook_{instance}"
