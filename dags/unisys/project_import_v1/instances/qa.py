# pylint: disable=wildcard-import unused-wildcard-import
from unisys.project_import_v1.config import *

instance = "qa"
environment = "pre-production"

version = "_v1"

# Unisys QA/UAT Configuration
company_key = "UnisysDev"

# File name validation prefix (for file name pattern validation)
file_name_prefix = "DEV"

# Connection IDs
replicon_conn_id = "unisysuat_replicon_admin"
sftp_conn_id = "sftp_internal_useast2"
pgp_conn_id = "unisys_pgp_key"

# SFTP Configuration (tech spec: username 710319_UAT, host rsftp-useast.replicon.com)
input_filepath = "/Unisys/Input/"
archive_filepath = "/Unisys/Archive/"
sftp_log_path = "/Unisys/Logs/"

# Email Configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG IDs for this instance
main_dag_id = f"unisys_project_import_main_{instance}{version}"
process_project_dag_id = f"unisys_project_import_process_projects_child_{instance}{version}"

# Batch task control variable
can_run_batch_task_var_name = f"unisys_project_import_batch_task_enabled_{instance}"

# Decryption variable (set to 'true' if files are encrypted)
can_decrypt_file_var_name = f"unisys_project_import_can_decrypt_file_{instance}"

disabled=True
