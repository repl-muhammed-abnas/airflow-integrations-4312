# pylint: disable=wildcard-import unused-wildcard-import
from unisys.resource_assignment.config import *

region = "us-east-1"
environment = "production"

# Instance identification
instance = "prod"
company_key = "unisyscorporation"

# File name validation prefix - PROD_Assignment_YYYYMMDD_HHMMSS.csv.pgp
file_name_prefix = "PROD"

# Connection IDs (same as user import)
replicon_conn_id = "unisyscorporation_replicon_repliconint"
sftp_conn_id = "sftp_unisyscorporation_710319_prod"
pgp_conn_id = "unisyscorporation_pgp_key"

# SFTP Configuration
input_filepath = "/Inbound/Project and Assignment/Input/"
archive_filepath = "/Inbound/Project and Assignment/Archive/"
sftp_log_path = "/Inbound/Project and Assignment/Logs/"

# Email Configuration
tenant_email = 'Cynthia.Rachel@in.unisys.com,Srinivasa.Thota@in.unisys.com,Prashant.Vishwakarma@unisys.com,Unisysproject@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG IDs for this instance
main_dag_id = f"unisys_resource_assignment_main_{instance}"
process_assignment_dag_id = f"unisys_resource_assignment_process_assignment_child_{instance}"

# Batch task control variable
can_run_batch_task_var_name = f"unisys_resource_assignment_batch_task_enabled_{instance}"

# Decryption variable (set to 'true' if files are encrypted)
can_decrypt_file_var_name = f"unisys_resource_assignment_can_decrypt_file_{instance}"
