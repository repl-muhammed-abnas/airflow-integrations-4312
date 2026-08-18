# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_customer.config import *

environment = 'pre-production'

instance = "uat"

company_key = "alvarezandmarsalholdingsuat"
bearer_token_var = 'alvarezandmarsalholdingsuat_project_import_customer_token'

replicon_conn_id = "alvarezandmarsalholdingsuat_replicon_radmin.1"

project_import_customer_webhook_main_dag = f"alvarezandmarsalholdingsuat_project_import_customer_webhook_{instance}"

project_master_dag = "alvarezandmarsalholdings_project_import_customer_master_v1_uat"
