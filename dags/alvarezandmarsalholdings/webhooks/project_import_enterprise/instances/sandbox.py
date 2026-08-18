# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_enterprise.config import *

environment = 'pre-production'

instance = "sandbox"

company_key = "alvarezandmarsalsb"
bearer_token_var = 'alvarezandmarsalsb_enterprise_project_import_token'

replicon_conn_id = "alvarezandmarsalsb_replicon_repliconint.projectimport"

project_import_enterprise_webhook_main_dag = f"alvarezandmarsalsb_project_import_enterprise_webhook_{instance}"

project_master_dag = f"alvarezandmarsalholdings_enterprise_project_import_master_v2_{instance}"

trigger_dag_id_var = f'alvarezandmarsalholdings_enterprise_project_import_trigger_dag_id_var_{instance}'
