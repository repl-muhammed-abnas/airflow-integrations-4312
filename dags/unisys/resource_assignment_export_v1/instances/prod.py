from unisys.resource_assignment_export_v1.config import *

region = "us-east-1"
environment = "production"

# Instance identification
instance = "prod"
company_key = "unisyscorporation"

# Connection IDs (same as user import)
replicon_conn_id = 'unisyscorporation_replicon_repliconint'
sftp_conn_id = 'sftp_unisyscorporation_710319_prod'
pgp_conn_id = 'pgp_unisys_time_export_oracle_prod'
secondary_pgp_conn_id = 'pgp_unisys_time_export_oracle_secondary_prod'

# SFTP Configuration
sftp_remote_path = '/Outbound/DataLake/'
sftp_ops_remote_path = '/Outbound/DataLake_Ops'

# Log file path
sftp_logs_filepath = '/Outbound/DataLake/Logs'

version = '_v1'
dag_id_suffix = f'{instance}{version}'

# Webhook DAG ID
allocation_details_child_dag_id = f'unisys_resource_assignment_export_child_{dag_id_suffix}'
scheduled_master_dag_id = f'unisys_resource_assignment_export_master_{dag_id_suffix}'

# Webhook configuration
webhook_log_name = f"unisys_resource_assignment_webhooks_{instance}"

# Email configuration
tenant_email = 'Cynthia.Rachel@in.unisys.com,Srinivasa.Thota@in.unisys.com,Prashant.Vishwakarma@unisys.com,Unisysproject@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Enable PGP encryption for export files
enable_encryption = True
