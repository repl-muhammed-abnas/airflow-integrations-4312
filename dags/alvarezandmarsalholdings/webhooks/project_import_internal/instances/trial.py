# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_internal.config import *

environment = 'pre-production'

instance = "trial"

company_key = "alvarezandmarsalholdingssandbox"
bearer_token_var = 'alvarezandmarsalholdingssandbox_project_import_internal_token'

replicon_conn_id = "alvarezandmarsalholdingssandbox_replicon_radmin1"

project_import_internal_webhook_main_dag = f"alvarezandmarsalholdingssandbox_project_import_internal_webhook_{instance}"

disabled=True
