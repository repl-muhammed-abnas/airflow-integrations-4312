from tsystems.activity_type_import.config import *

instance = "prod"

company_key = "tsystems"
environment = "production"

replicon_conn_id = "tsystems_replicon_repliconint.userimport"

#Client SFTP Connection
sftp_conn_id = "sftp_tsystems_Replicon_ICM"

# File paths and SFTP configuration
input_filepath = '/PROD/IN/Activity Type Import Integration/INPUT'
archive_filepath = '/PROD/IN/Activity Type Import Integration/ARCHIVE'
log_filepath = '/PROD/IN/Activity Type Import Integration/LOGS'

# Email notification settings
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "" # _v1, _v2 etc.

dag_id_suffix = f"{instance}{version}"

master_dag_id = f"tsystems_activity_type_import_master_{dag_id_suffix}"
process_each_record_dagid = f"tsystems_activity_type_import_process_each_record_child_{dag_id_suffix}"
process_new_division_dagid = f"tsystems_activity_type_import_process_new_division_child_{dag_id_suffix}"

can_run_batch_task_var_name = "tsystems_activity_type_import_batch_task"
