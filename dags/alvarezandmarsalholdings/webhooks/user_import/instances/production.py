from alvarezandmarsalholdings.webhooks.user_import.config import *

instance = 'prod'
environment = 'production'
company_key = 'alvarezandmarsal'
replicon_conn_id = 'alvarezandmarsal_replicon_repliconint.userimport'

bearer_token_var = 'alvarezandmarsal_user_import_webhook_token'

user_import_webhook_main_dag = f"alvarezandmarsalholdings_user_import_webhook_{instance}"

user_import_master_dag_id = f'alvarezandmarsalholdings_user_import_master_{instance}_v2'
