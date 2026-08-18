# pylint: disable=wildcard-import unused-wildcard-import
from procore_ce_integration.vendors_sync.config import *

instance = "dev"

# Connection IDs for trial environment
computerease_conn_id = f'computerease_{instance}'
procore_conn_id = f'procore_{instance}'

vendor_webhook_dag_id = f'procore_computerease_vendor_webhook_{instance}'
bearer_token_var = f'procore_ce_webhook_token_{instance}'

# Acceptance-gated origin_id update — enabled for testing before default flip.
defer_origin_id_until_accepted = True

# Email configuration
tenant_email = 'MPTeamReplicon@deltek.com'
internal_email = 'MPTeamReplicon@deltek.com'

