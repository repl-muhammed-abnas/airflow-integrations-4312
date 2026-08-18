from nber.webhooks.config import *

instance = "uat"
replicon_conn_id = "nbertrial01_replicon_repliconint"
company_key = "nbertrial01"
process_payload_dagid = f"nber_project_import_master_{instance}"
master_dagid = f"nber_project_import_webhook_master_{instance}"

nber_bearer_token=f"nber_project_import_bearer_token_{instance}"