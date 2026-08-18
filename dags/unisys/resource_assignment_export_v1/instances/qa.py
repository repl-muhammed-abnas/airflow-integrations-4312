from unisys.resource_assignment_export_v1.config import *

instance = 'qa'

company_key = 'unisysdev'

# Connection IDs
replicon_conn_id = 'unisysdev_replicon_repliconint'
sftp_conn_id = 'sftp_internal_useast2'
pgp_conn_id = 'unisys_pgp_key'
secondary_pgp_conn_id = 'pgp_unisys_time_export_oracle_secondary_dev'

sftp_remote_path = '/Unisys/ResourceAssignments/Export/'
sftp_ops_remote_path = '/Unisys/Outbound/DataLake_Ops'

# Log file path
sftp_logs_filepath = '/Unisys/Outbound/DataLake/Logs'

version = '_v1'
dag_id_suffix = f'{instance}{version}'

# Webhook DAG ID
webhook_master_dag_id = f'unisys_resource_assignment_export_webhook_{instance}'
allocation_details_child_dag_id = f'unisys_resource_assignment_export_child_{dag_id_suffix}'
scheduled_master_dag_id = f'unisys_resource_assignment_export_master_{dag_id_suffix}'

# Webhook configuration
webhook_log_name = f"unisys_resource_assignment_webhooks_{instance}"

# Email configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

enable_encryption = False
