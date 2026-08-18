# pylint: disable=wildcard-import unused-wildcard-import
from kpmg_au_userimport_webhook.config import *

instance = "dev"
environment = 'pre-production'
region = 'eu-central-1'

company_key = "KPMGAUDEV"

bearer_token_var = f"kpmg_australia_webhook_token_variable_{instance}"

replicon_conn_id = "kpmg_replicon_replicon.admin"

webhook_main_dag_id = f"kpmg_australia_userimport_webhook_master_{instance}"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
