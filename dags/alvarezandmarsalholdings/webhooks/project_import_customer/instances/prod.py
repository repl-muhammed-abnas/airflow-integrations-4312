# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_customer.config import *

environment = 'production'

instance = "production"

company_key = "alvarezandmarsal"
bearer_token_var = 'alvarezandmarsal_project_import_customer_token'

replicon_conn_id = "alvarezandmarsal_replicon_repliconint.projectimport"

project_import_customer_webhook_main_dag = f"alvarezandmarsal_project_import_customer_webhook_{instance}"

project_master_dag = "alvarezandmarsalholdings_project_import_customer_master_v1_production"
