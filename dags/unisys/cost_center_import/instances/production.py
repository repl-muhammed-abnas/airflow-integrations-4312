# pylint: disable=wildcard-import unused-wildcard-import
from unisys.cost_center_import.config import *

# Instance identification
instance = "prod"
company_key = "UnisysCorporation"
environment = "production"
replicon_conn_id = "unisyscorporation_replicon_repliconint"
pgp_conn_id = "unisyscorporation_pgp_key"

# SFTP configuration
# Based on Unisys integration patterns
sftp_conn_id = "sftp_unisysprod_710319_prod"
input_filepath = "/Inbound/Cost Center/Input"
archive_filepath = "/Inbound/Cost Center/Archive"
log_filepath = "/Inbound/Cost Center/Logs"

# Email configuration
tenant_email = 'Prashant.Vishwakarma@unisys.com,Raviraj.Ramachandra@in.unisys.com,Unisysproject@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},Unisysproject@deltek.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG identifiers
master_dag_id = f"unisys_cost_center_import_master_{instance}"
process_cost_centers_child_dag_id = f"unisys_cost_center_import_process_cost_centers_child_{instance}"
process_company_code_child_dag_id = f"unisys_cost_center_import_process_company_code_child_{instance}"

# Feature flags - Control variables for runtime behavior
can_decrypt_file_var_name = f"unisys_cost_center_import_can_decrypt_file_{instance}"
can_run_batch_task = f"unisys_cost_center_import_can_run_batch_task_{instance}"
