# pylint: disable=wildcard-import unused-wildcard-import
from procore_ce_integration.vendors_sync.config import *

instance = "farris"
region = 'us-east-1'
environment = 'pre-production'

# Connection IDs
computerease_conn_id = f'computerease_{instance}'
procore_conn_id = f'procore_{instance}'

vendor_webhook_dag_id = f'procore_computerease_vendor_webhook_{instance}'
bearer_token_var = f'procore_ce_webhook_token_{instance}'


# Email configuration
tenant_email = ['mick.hodgins@fiicgc.com']
internal_email = ['procoreintegrationsupport@deltek.com']