"""
UAT Instance Configuration - Unisys Cost Center Import Integration

Instance-specific configuration for the UAT (User Acceptance Testing) environment
of the Unisys Cost Center Import integration. This module inherits base configuration
from config.py and overrides instance-specific settings.

Configuration includes:
    - Instance identification and naming
    - Replicon connection settings
    - SFTP file transfer configuration
    - Email notification settings
    - DAG identifiers for master and child DAGs
    - Feature flags and control variables

Key Settings:
    - instance: 'uat'
    - company_key: 'UnisysUAT'
    - Region: us-east-1
    - SFTP paths for input, archive, and logs

Note:
    This is a UAT instance configuration for pre-production testing.
"""

# pylint: disable=wildcard-import unused-wildcard-import
from unisys.cost_center_import.config import *

# Instance identification
instance = "uat"
company_key = "UnisysUAT"
replicon_conn_id = "unisysuat_replicon_repliconint"
pgp_conn_id = "unisys_pgp_key"

# SFTP configuration
# Based on Unisys integration patterns
sftp_conn_id = "sftp_unisysuat_710319_UAT"
input_filepath = "/Inbound/Cost Center/Input"
archive_filepath = "/Inbound/Cost Center/Archive"
log_filepath = "/Inbound/Cost Center/Logs"

# Email configuration
tenant_email = 'Prashant.Vishwakarma@unisys.com,Raviraj.Ramachandra@in.unisys.com,Unisysproject@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},Unisysproject@deltek.com'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

# DAG identifiers
master_dag_id = f"unisys_cost_center_import_master_{instance}"
process_cost_centers_child_dag_id = f"unisys_cost_center_import_process_cost_centers_child_{instance}"
process_company_code_child_dag_id = f"unisys_cost_center_import_process_company_code_child_{instance}"

# Feature flags - Control variables for runtime behavior
can_decrypt_file_var_name = f"unisys_cost_center_import_can_decrypt_file_{instance}"
can_run_batch_task = f"unisys_cost_center_import_can_run_batch_task_{instance}"
