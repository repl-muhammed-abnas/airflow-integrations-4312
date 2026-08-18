"""Trial instance configuration for T-Systems Time Import."""
from tsystems.time_import_v1.config import *
from tsystems.time_import_v1.mappers.timesheet_mapper import timesheet_templates

instance = "trial"

company_key = "tsystemsSB"
environment = "pre-production"

# File paths and SFTP configuration
input_filepath = '/shivam/TsystemsSB/time_import/input'
archive_filepath = '/shivam/TsystemsSB/time_import/archive'
log_filepath = '/shivam/TsystemsSB/time_import/logs'

# Email notification settings
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id = "tsystems_replicon_replicon.admin"
sftp_conn_id = "sftp_useast2"

version = "_v1" # _v1, _v2 etc.

dag_id_prefix = f"{instance}{version}"

# DAG IDs
master_dagid = f"tsystems_time_import_master_{dag_id_prefix}"
process_unique_users_child = f"tsystems_time_import_process_users_child_{dag_id_prefix}"
process_each_entry_child = f"tsystems_time_import_process_each_entry_child_{dag_id_prefix}"
process_each_inout_child = f"tsystems_time_import_process_each_inout_child_{dag_id_prefix}"
process_log_generation = f"tsystems_time_import_log_generation_{dag_id_prefix}"

can_run_batch_task = f"tsystems_time_import_can_run_batch_task_{dag_id_prefix}_var"

TIMESHEET_TEMPLATES = timesheet_templates

disabled = True
