from nber.webhooks.config import *

instance = "production"
environment = "production"
replicon_conn_id = "nber_replicon_repliconint"
company_key = "nber"
process_payload_dagid = f"nber_project_import_master_{instance}"
master_dagid = f"nber_project_import_webhook_master_{instance}"

nber_bearer_token=f"nber_project_import_bearer_token_{instance}"