# pylint: disable=wildcard-import unused-wildcard-import
from guidehouse.resource_assignment.config import *

# Instance identification
instance = 'sit'
environment = "pre-production"

company_key = 'GuideHouseIncSB2'

# SFTP configuration
sftp_conn_id = 'sftp_guidehousesb2_678659_uat'
replicon_conn_id = 'guidehousesb2_replicon_repliconint'
pgp_conn_id = 'guidehousesb2_replicon_pgp_conn_inbound'


# File name validation prefix (pattern: <file_name_prefix>_YYYYMMDDHHSS.txt.pgp)
file_name_prefix = "PPS_Project_team"

input_filepath = '/SIT/Inbound/PS Project and Workforce/Input'
archive_filepath = '/SIT/Inbound/PS Project and Workforce/Archive'
sftp_log_path = '/SIT/Inbound/PS Project and Workforce/Logs'

# Email configuration
tenant_email = 'guidehousedeltekprojectteam@deltek.com,ghcostpoint@guidehouse.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "" # _v1, _v2, etc.
postfix = f"{instance}{version}"

# DAG IDs
main_dag_id = f"guidehouse_resource_assignment_main_{postfix}"
process_assignment_dag_id = f"guidehouse_resource_assignment_process_assignment_child_{postfix}"

# Batch task control variable
can_run_batch_task_var_name = f"guidehouse_resource_assignment_batch_task_enabled_{postfix}"

# Decryption variable (set to 'true' if files are encrypted)
can_decrypt_file_var_name = f"guidehouse_resource_assignment_can_decrypt_file_{postfix}"
