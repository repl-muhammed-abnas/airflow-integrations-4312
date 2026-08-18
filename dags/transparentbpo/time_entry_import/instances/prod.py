from transparentbpo.time_entry_import.config import *

instance = "production"

company_key = "TransparentBPO"
environment = "production"

input_filepath = '/Production/Timesheet Feed Files'
archive_filepath = '/Production/Timesheet Feed Archive'
log_filepath = '/Production/Timesheet Feed Logs'

tenant_email = 'replicon.crm.automation@transparentbpo.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id = "transparentbpo_replicoint"
sftp_conn_id = "sftp_transparentbpo_662208_PROD"

version = ""

dag_id_prefix = f"{instance}{version}"

master_dagid = f"transparentbpo_time_import_master_{dag_id_prefix}"
process_unique_users_child = f"transparentbpo_time_import_process_users_child_{dag_id_prefix}"
process_each_entry_date_child = f"transparentbpo_time_import_process_each_entry_date_child_{dag_id_prefix}"
process_each_time_entry_child = f"transparentbpo_time_import_process_each_time_entry_child_{dag_id_prefix}"
process_log_generation = f"transparentbpo_time_import_log_generation_{dag_id_prefix}"

can_run_batch_task = f"transparentbpo_time_import_can_run_batch_task_{dag_id_prefix}_var"
