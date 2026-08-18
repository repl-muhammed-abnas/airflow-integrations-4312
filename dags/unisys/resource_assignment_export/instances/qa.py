from unisys.resource_assignment_export.config import *

instance = 'qa'

company_key = 'unisysdev'

# Connection IDs
replicon_conn_id = 'unisysdev_replicon_repliconint'
sftp_conn_id = 'sftp_internal_useast2'
pgp_conn_id = 'unisys_pgp_key'

sftp_remote_path = '/Unisys/ResourceAssignments/Export/'

# Webhook DAG ID
webhook_master_dag_id = f'unisys_resource_assignment_export_webhook_{instance}'
allocation_details_child_dag_id = f'unisys_resource_assignment_export_child_{instance}'
scheduled_master_dag_id = f'unisys_resource_assignment_export_master_{instance}'

# Webhook configuration
webhook_log_name = f"unisys_resource_assignment_webhooks_{instance}"

# Email configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

enable_encryption = False
