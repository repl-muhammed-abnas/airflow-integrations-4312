# pylint: disable=wildcard-import unused-wildcard-import
from tsystems.webhooks.office_schedule_api_import.config import *

environment = 'pre-production'

instance = "uat"

company_key = "TsystemsSB"

bearer_token_var = 'tsystemssb_office_schedule_sync_api_webhook_token'

replicon_conn_id = "TsystemsSB_replicon_replicon.admin"

webhook_main_dag_id = f"tsystems_office_schedule_sync_api_import_webhook_{instance}"
trigger_master_dag_id = f'tsystems_office_schedule_api_import_master_{instance}_v1'

