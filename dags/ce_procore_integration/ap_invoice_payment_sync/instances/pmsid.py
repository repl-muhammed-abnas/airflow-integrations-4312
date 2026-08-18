from ce_procore_integration.ap_invoice_payment_sync.config import *

instance = 'pmsid'

procore_conn_id = f'procore_{instance}'
sftp_conn_id = f'ce_procore_sftp_{instance}'
computerease_conn_id = f'computerease_{instance}'

input_source = InputSource.SFTP

# S3 configuration for MD5 fingerprint storage
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'
s3_file_name = 'ap_invoice_payment_fingerprints.csv'
s3_fingerprints_prefix = f'CE_Procore/production/{instance}'
s3_fingerprints_key = f'{s3_fingerprints_prefix}_{s3_file_name}'

# DAG IDs
main_dag_id = f'computerease_procore_ap_invoice_payment_sync_main_{instance}'
child_dag_id = f'computerease_procore_ap_invoice_payment_sync_child_{instance}'
payment_dag_id = f'computerease_procore_ap_invoice_payment_sync_payment_{instance}'

# File path configuration
file_path = f'/ce_procore/ap_invoice_payments/{instance}'
archive_file_path = f'{file_path}/archive'

# Email configuration
tenant_email = ['SiddhantrajSingh@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
