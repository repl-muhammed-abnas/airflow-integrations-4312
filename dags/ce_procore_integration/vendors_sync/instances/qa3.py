# pylint: disable=wildcard-import unused-wildcard-import
from ce_procore_integration.vendors_sync.config import *

instance = 'qa3'
environment = 'qa'

# Connection IDs
computerease_conn_id = f'computerease_{instance}'
procore_conn_id = f'procore_{instance}'

vendor_main_dag_id = f'computerease_procore_vendor_sync_main_{instance}'
vendor_child_dag_id = f'computerease_procore_vendor_sync_child_{instance}'

vendor_last_sync_time_var = f'ce_procore_vendor_sync_last_sync_time_{instance}'

# Email configuration
tenant_email = 'MPTeamReplicon@deltek.com'
internal_email = 'MPTeamReplicon@deltek.com'
