from kpmg_au_userimport_webhook_test.config import *

instance = "qa"
environment = 'qa'

region = 'us-east-1'

company_key = "KPMGAUTrial"

bearer_token_var = f"kpmg_australia_webhook_token_variable_{instance}"

replicon_conn_id = "KPMG_replicon_replicon.admin"

webhook_main_dag_id = f"kpmg_australia_userimport_webhook_master_{instance}"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'