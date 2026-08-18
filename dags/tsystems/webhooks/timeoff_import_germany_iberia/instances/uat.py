# pylint: disable=wildcard-import unused-wildcard-import
from tsystems.webhooks.timeoff_import_germany_iberia.config import *

# Instance configuration
instance = "uat"
environment = 'pre-production'

# Company settings
company_key = "TSystemsSB"

# Bearer token variable name for webhook authentication
bearer_token_var = 'tsystemssb_timeoff_import_germany_iberia_webhook_token_uat'

# Connection IDs
replicon_conn_id = "TSystemsSB_replicon_replicon.admin"

# DAG IDs
webhook_main_dag_id = f"tsystems_timeoff_import_germany_iberia_webhook_{instance}"
trigger_master_dag_id = f'tsystems_timeoff_import_germany_iberia_master_{instance}_v1'

# Email configuration for this instance
tenant_email = "TSI_Replicon@t-systems.com"