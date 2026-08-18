# pylint: disable=wildcard-import
from ce_procore_integration.ap_invoice_sync.config import *

instance = 'pmdemo2'
region = 'us-east-1'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = f'ce_procore_sftp_{instance}'

main_dag_id = f'computerease_procore_ap_invoice_sync_main_{instance}'
child_dag_id = f'computerease_procore_ap_invoice_sync_child_{instance}'

input_source = 'sftp'

file_path = '/ce_procore/ap_invoices'
archive_filepath = f'{file_path}/archive'
ap_invoice_report_filename = 'AP Invoice Detail Report'

tenant_email = ['christinehill@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
