from unisys.resource_assignment_export_v1.config import *

instance = 'sit'

company_key = 'Unisysdev'

# Connection IDs
replicon_conn_id = 'unisysdev_replicon_repliconint'
sftp_conn_id = 'unisys_fieldglass_sftp_710319'
pgp_conn_id = 'pgp_unisys_time_export_oracle_uat'
secondary_pgp_conn_id = 'pgp_unisys_time_export_oracle_secondary_uat'

sftp_remote_path = '/Outbound/DataLake/'
sftp_ops_remote_path = '/Outbound/DataLake_Ops'

version = '_v1'
dag_id_suffix = f'{instance}{version}'

# Log file path
sftp_logs_filepath = '/Outbound/DataLake/Logs'

# DAG IDs
allocation_details_child_dag_id = f'unisys_resource_assignment_export_child_{dag_id_suffix}'
scheduled_master_dag_id = f'unisys_resource_assignment_export_master_{dag_id_suffix}'

# Webhook configuration
webhook_log_name = f"unisys_resource_assignment_webhooks_{instance}"

# Email configuration
tenant_email = 'Unisysproject@deltek.com,Prashant.Vishwakarma@unisys.com,Raviraj.Ramachandra@in.unisys.com'

enable_encryption = True
