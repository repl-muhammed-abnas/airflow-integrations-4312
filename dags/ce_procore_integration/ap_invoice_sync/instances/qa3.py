# pylint: disable=wildcard-import
from ce_procore_integration.ap_invoice_sync.config import *

instance = 'qa3'
environment = 'qa'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

input_source = 'email'
imap_conn_id = f'computerease_procore_imap_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_ap_invoice_sync_main_{instance}'
child_dag_id = f'computerease_procore_ap_invoice_sync_child_{instance}'

# Email configuration
tenant_email = 'MPTeamReplicon@deltek.com'
internal_email = 'MPTeamReplicon@deltek.com'
