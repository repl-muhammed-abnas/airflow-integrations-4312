from tsystems.jira_time_import_v1.config import *

instance = "prod"

environment = 'production'

company_key = 'Tsystems'

replicon_conn_id = "tsystems_replicon_repliconint.timeimport"
sftp_conn_id = "sftp_tsystems_Replicon_JIRA"

input_filepath = '/PROD/INPUT'
archive_filepath = '/PROD/ARCHIVE'
log_filepath = '/PROD/LOGS'

tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "_v1" # _v1, _v2 etc.

dag_id_prefix = f"{instance}{version}"

master_dagid = f"tsystems_jira_time_import_master_{dag_id_prefix}"
process_unique_users_child = f"tsystems_jira_time_import_process_users_child_{dag_id_prefix}"
process_each_entry_child = f"tsystems_jira_time_import_process_each_entry_child_{dag_id_prefix}"
process_log_generation = f"tsystems_jira_time_import_log_generation_{dag_id_prefix}"
send_email_notification_child = f"tsystems_jira_time_import_send_email_notification_child_{dag_id_prefix}"

can_run_batch_task = f"tsystems_jira_time_import_can_run_batch_task_{dag_id_prefix}_var"
