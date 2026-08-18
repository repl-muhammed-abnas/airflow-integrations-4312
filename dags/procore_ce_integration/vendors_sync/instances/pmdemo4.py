# pylint: disable=wildcard-import unused-wildcard-import
from procore_ce_integration.vendors_sync.config import *

instance = "pmdemo4"

# Connection IDs for pre-production environment
computerease_conn_id = f'computerease_{instance}'
procore_conn_id = f'procore_{instance}'

vendor_webhook_dag_id = f'procore_computerease_vendor_webhook_{instance}'
bearer_token_var = f'procore_ce_webhook_token_{instance}'


# Email configuration
tenant_email = ['christinehill@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

