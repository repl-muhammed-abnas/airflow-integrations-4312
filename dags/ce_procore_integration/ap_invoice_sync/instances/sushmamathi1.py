# pylint: disable=wildcard-import
from ce_procore_integration.ap_invoice_sync.config import *
from ce_procore_integration.util_dags.constants import InputSource

instance = 'sushmamathi1'
region = 'us-east-1'
environment = 'pre-production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = f'ce_procore_sftp_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_ap_invoice_sync_main_{instance}'
child_dag_id = f'computerease_procore_ap_invoice_sync_child_{instance}'

# Input source configuration
input_source = InputSource.SFTP
file_path = '/ce_procore/ap_invoices'
archive_filepath = f'{file_path}/archive'
ap_invoice_report_filename = 'Procore AP Invoices 1'

# Email configuration
tenant_email = ['sushmamathi@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
