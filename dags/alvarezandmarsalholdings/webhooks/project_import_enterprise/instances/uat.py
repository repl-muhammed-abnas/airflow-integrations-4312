# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_enterprise.config import *

environment = 'pre-production'

instance = "uat"

company_key = "alvarezandmarsalholdingsuat"
bearer_token_var = 'alvarezandmarsalholdingsuat_project_import_enterprise_token'

replicon_conn_id = "alvarezandmarsalholdingsuat_replicon_radmin.1"

project_import_enterprise_webhook_main_dag = f"alvarezandmarsalholdingsuat_project_import_enterprise_webhook_{instance}"

project_master_dag = "alvarezandmarsalholdings_enterprise_project_import_master_v2_uat"

trigger_dag_id_var = f'alvarezandmarsalholdings_enterprise_project_import_trigger_dag_id_var_{instance}'
