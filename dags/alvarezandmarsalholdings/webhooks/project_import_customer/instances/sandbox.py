# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_customer.config import *

environment = 'pre-production'

instance = "sandbox"

company_key = "alvarezandmarsalsb"
bearer_token_var = 'alvarezandmarsalsb_project_import_customer_token'

replicon_conn_id = "alvarezandmarsalsb_replicon_repliconint.projectimport"

project_import_customer_webhook_main_dag = f"alvarezandmarsal_project_import_customer_webhook_{instance}"

project_master_dag = f"alvarezandmarsalholdings_project_import_customer_master_v1_{instance}"
