# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_enterprise.config import *

environment = 'pre-production'

instance = "dev"

company_key = "alvarezandmarsalholdingsdev"
bearer_token_var = 'alvarezandmarsalholdingsdev_project_import_enterprise_token'

replicon_conn_id = "alvarezandmarsalholdingsdev_replicon_radmin1"

project_import_enterprise_webhook_main_dag = f"alvarezandmarsalholdingsdev_project_import_enterprise_webhook_{instance}"

project_master_dag = "alvarezandmarsalholdings_enterprise_project_import_master_v4_sit"

trigger_dag_id_var = f'alvarezandmarsalholdings_enterprise_project_import_trigger_dag_id_var_{instance}'
