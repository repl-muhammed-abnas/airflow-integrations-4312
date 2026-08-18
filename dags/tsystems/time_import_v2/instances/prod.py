"""Production instance configuration for T-Systems Time Import."""
from tsystems.time_import_v2.config import *
from tsystems.time_import_v2.mappers.timesheet_mapper import timesheet_templates

instance = "production"

company_key = "Tsystems"
environment = "production"

# File paths and SFTP configuration
input_filepath = '/PROD/INPUT'
archive_filepath = '/PROD/ARCHIVE'
log_filepath = '/PROD/LOGS'

# Email notification settings
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id = "tsystems_replicon_repliconint.timeimport"
sftp_conn_id = "sftp_tsystems_Replicon_Timesheets_Import"

version = "_v2"

dag_id_prefix = f"{instance}{version}"

# DAG IDs
master_dagid = f"tsystems_time_import_master_{dag_id_prefix}"
process_unique_users_child = f"tsystems_time_import_process_users_child_{dag_id_prefix}"
process_each_entry_child = f"tsystems_time_import_process_each_entry_child_{dag_id_prefix}"
process_each_inout_child = f"tsystems_time_import_process_each_inout_child_{dag_id_prefix}"
process_log_generation = f"tsystems_time_import_log_generation_{dag_id_prefix}"

can_run_batch_task = f"tsystems_time_import_can_run_batch_task_{dag_id_prefix}_var"

TIMESHEET_TEMPLATES = timesheet_templates

include_username_in_logs = False
