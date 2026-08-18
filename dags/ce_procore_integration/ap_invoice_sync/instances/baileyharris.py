# pylint: disable=wildcard-import
from ce_procore_integration.ap_invoice_sync.config import *

instance = 'baileyharris'
region = 'us-east-1'
environment = 'pre-production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_ap_invoice_sync_main_{instance}'
child_dag_id = f'computerease_procore_ap_invoice_sync_child_{instance}'

# Email configuration
tenant_email = ['tiffany.blackmon@baileyharris.com']
internal_email = ['procoreintegrationsupport@deltek.com']

# Input source configuration
input_source = 'email'
imap_conn_id = f'computerease_procore_imap_{instance}'
email_subject_pattern = 'AP Invoices Procore Report'
ap_invoice_report_filename = 'AP Invoices Procore Report'
