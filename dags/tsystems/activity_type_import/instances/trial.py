from tsystems.activity_type_import.config import *

instance = "trial"

company_key = "tsystemsSB"
environment = "pre-production"

replicon_conn_id = "tsystems_replicon_replicon.admin"
sftp_conn_id = "sftp_useast2"

# File paths and SFTP configuration
input_filepath = '/TsystemsSB/activity_type_import/input'
archive_filepath = '/TsystemsSB/activity_type_import/archive'
log_filepath = '/TsystemsSB/activity_type_import/logs'

# Email notification settings
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "" # _v1, _v2 etc.

dag_id_prefix = f"{instance}{version}"

master_dag_id = f"tsystems_activity_type_import_master_{dag_id_prefix}"
process_each_record_dagid = f"tsystems_activity_type_import_process_each_record_child_{dag_id_prefix}"
process_new_division_dagid = f"tsystems_activity_type_import_process_new_division_child_{dag_id_prefix}"

can_run_batch_task_var_name = "tsystems_activity_type_import_batch_task"
