from unisys.resource_assignment_export.config import *

instance = 'uat'

company_key = 'UnisysUAT'

# Connection IDs
replicon_conn_id = 'unisysuat_replicon_repliconint'
sftp_conn_id = 'sftp_unisysuat_710319_UAT'
pgp_conn_id = 'pgp_unisys_time_export_oracle_uat'

sftp_remote_path = '/Outbound/DataLake/'

# DAG IDs
allocation_details_child_dag_id = f'unisys_resource_assignment_export_child_{instance}'
scheduled_master_dag_id = f'unisys_resource_assignment_export_master_{instance}'

# Webhook configuration
webhook_log_name = f"unisys_resource_assignment_webhooks_{instance}"

# Email configuration
tenant_email = 'Unisysproject@deltek.com,Prashant.Vishwakarma@unisys.com,Raviraj.Ramachandra@in.unisys.com'

enable_encryption = True

disabled=True
