# pylint: disable=wildcard-import unused-wildcard-import
from unisys.resource_assignment.config import *

# Instance identifier
instance = "uat"
environment = "pre-production"

# Unisys UAT Configuration
company_key = "UnisysUAT"

# File name validation prefix (for file name pattern validation)
file_name_prefix = "UAT"

# Connection IDs
replicon_conn_id = "unisysuat_replicon_repliconint"
sftp_conn_id = "sftp_unisysuat_710319_UAT"
pgp_conn_id = "unisys_pgp_key"

# Email Configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = "/Inbound/Project and Assignment/Input/"
archive_filepath = "/Inbound/Project and Assignment/Archive/"
sftp_log_path = "/Inbound/Project and Assignment/Logs/"

# DAG IDs
main_dag_id = f"unisys_resource_assignment_main_{instance}"
process_assignment_dag_id = f"unisys_resource_assignment_process_assignment_child_{instance}"

# Batch task control variable
can_run_batch_task_var_name = f"unisys_resource_assignment_batch_task_enabled_{instance}"

# Decryption variable (set to 'true' if files are encrypted)
can_decrypt_file_var_name = f"unisys_resource_assignment_can_decrypt_file_{instance}"
