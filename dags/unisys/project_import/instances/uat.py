# pylint: disable=wildcard-import unused-wildcard-import
from unisys.project_import.config import *

instance = "uat"
environment = "pre-production"

# Unisys QA/UAT Configuration
company_key = "UnisysUAT"

# File name validation prefix (for file name pattern validation)
file_name_prefix = "UAT"

# Connection IDs
replicon_conn_id = "unisysuat_replicon_repliconint"
sftp_conn_id = "sftp_unisysuat_710319_UAT"
pgp_conn_id = "unisysuat_replicon_pgp_conn"

# SFTP Configuration (tech spec: username 710319_UAT, host rsftp-useast.replicon.com)
input_filepath = "/Inbound/Project and Assignment/Input/"
archive_filepath = "/Inbound/Project and Assignment/Archive/"
sftp_log_path = "/Inbound/Project and Assignment/Logs/"

# Email Configuration
tenant_email = 'Prashant.Vishwakarma@unisys.com,Raviraj.Ramachandra@in.unisys.com,Unisysproject@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},Unisysproject@deltek.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG IDs for this instance
main_dag_id = f"unisys_project_import_main_{instance}"
process_project_dag_id = f"unisys_project_import_process_projects_child_{instance}"

# Batch task control variable
can_run_batch_task_var_name = f"unisys_project_import_batch_task_enabled_{instance}"

# Decryption variable (set to 'true' if files are encrypted)
can_decrypt_file_var_name = f"unisys_project_import_can_decrypt_file_{instance}"

disabled=True
