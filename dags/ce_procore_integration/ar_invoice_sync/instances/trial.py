# pylint: disable=wildcard-import
from ce_procore_integration.ar_invoice_sync.config import *
from ce_procore_integration.ar_invoice_sync.utils.constants import InputSource

instance = 'dev'

input_source = InputSource.EMAIL

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = 'repliconsftp'

# DAG IDs
main_dag_id = f'computerease_procore_ar_invoice_sync_main_{instance}'
child_dag_id = f'computerease_procore_ar_invoice_sync_child_{instance}'
invoice_dag_id = f'computerease_procore_ar_invoice_sync_owner_invoice_{instance}'
prime_contract_sov_dag_id = f'computerease_procore_ar_invoice_sync_prime_contract_sov_{instance}'

# Email input source configuration (used when input_source = 'email')
imap_conn_id = 'computerease_procore_imap'
email_subject_pattern = 'AR Invoice Report'
email_limit = 1
email_max_to_check = 100

# Email notification configuration
tenant_email = ['MPTeamReplicon@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
