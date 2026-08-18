# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.webhooks.project_import_enterprise.config import *

environment = 'production'

instance = "production"

company_key = "alvarezandmarsal"
bearer_token_var = 'alvarezandmarsal_enterprise_project_import_token'

replicon_conn_id = "alvarezandmarsal_replicon_repliconint.projectimport"

project_import_enterprise_webhook_main_dag = f"alvarezandmarsal_project_import_enterprise_webhook_{instance}"

project_master_dag = "alvarezandmarsalholdings_enterprise_project_import_master_v2_production"

trigger_dag_id_var = f'alvarezandmarsalholdings_enterprise_project_import_trigger_dag_id_var_{instance}'
