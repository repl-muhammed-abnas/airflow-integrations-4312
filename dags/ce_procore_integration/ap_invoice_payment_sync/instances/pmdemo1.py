from ce_procore_integration.ap_invoice_payment_sync.config import *
from ce_procore_integration.ap_invoice_payment_sync.utils.constants import InputSource

instance = 'pmdemo1'
region = 'us-east-1'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
imap_conn_id = f'computerease_procore_imap_{instance}'

input_source = InputSource.EMAIL

# S3 configuration for MD5 fingerprint storage
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'
s3_file_name = 'ap_invoice_payment_fingerprints.csv'
s3_fingerprints_prefix = f'CE_Procore/{environment}/{instance}'
s3_fingerprints_key = f'{s3_fingerprints_prefix}_{s3_file_name}'

# DAG IDs
main_dag_id = f'computerease_procore_ap_invoice_payment_sync_main_{instance}'
child_dag_id = f'computerease_procore_ap_invoice_payment_sync_child_{instance}'
payment_dag_id = f'computerease_procore_ap_invoice_payment_sync_payment_{instance}'

# Email configuration
tenant_email = ['timothymattlin@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
