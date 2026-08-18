# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_customer.config import *

environment = 'pre-production'

instance = "trial"

company_key = "alvarezandmarsalholdingsdev"
bearer_token_var = 'alvarezandmarsalholdingsdev_project_import_customer_token'

replicon_conn_id = "alvarezandmarsalholdingsdev_replicon_radmin1"

project_import_customer_webhook_main_dag = f"alvarezandmarsalholdingsdev_project_import_customer_webhook_{instance}"

project_master_dag = f"alvarezandmarsalholdings_project_import_customer_master_{instance}"
