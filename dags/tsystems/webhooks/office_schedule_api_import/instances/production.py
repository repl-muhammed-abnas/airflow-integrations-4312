# pylint: disable=wildcard-import unused-wildcard-import
from tsystems.webhooks.office_schedule_api_import.config import *

region = 'eu-central-1'
environment = 'production'

instance = "prod"

company_key = "tsystems"

bearer_token_var = 'tsystems_office_schedule_sync_api_webhook_token'

replicon_conn_id = "tsystems_replicon_repliconint.userimport"

webhook_main_dag_id = f"tsystems_office_schedule_sync_api_import_webhook_{instance}"
trigger_master_dag_id = f'tsystems_office_schedule_api_import_master_{instance}_v1'

