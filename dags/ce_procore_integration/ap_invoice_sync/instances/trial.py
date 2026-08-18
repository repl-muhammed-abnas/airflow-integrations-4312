# pylint: disable=wildcard-import
from ce_procore_integration.ap_invoice_sync.config import *

instance = 'dev'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = 'repliconsftp'

# DAG IDs
main_dag_id = f'computerease_procore_ap_invoice_sync_main_{instance}'
child_dag_id = f'computerease_procore_ap_invoice_sync_child_{instance}'

# Project configuration
file_path = '/ce_procore/ap_invoices'
archive_filepath = '/ce_procore/ap_invoices/archive'

# Email configuration
tenant_email = ['MPTeamReplicon@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

input_source = 'email'
