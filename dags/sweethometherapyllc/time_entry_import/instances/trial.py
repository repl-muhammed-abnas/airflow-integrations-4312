from sweethometherapyllc.time_entry_import.config import *

instance = "trial"

company_key = "sweethometherapyllctrial01"
environment = "pre-production"

input_filepath = '/sweethometherapyllc/time_import/input'
archive_filepath = '/sweethometherapyllc/time_import/archive'
log_filepath = '/sweethometherapyllc/time_import/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id = "sweethometherapyllc_inc_replicoint"
sftp_conn_id = "sftp_useast2"

version = ""

dag_id_prefix = f"{instance}{version}"

master_dagid = f"sweethometherapyllc_time_import_master_{dag_id_prefix}"
process_unique_therapists_child = f"sweethometherapyllc_time_import_process_therapists_child_{dag_id_prefix}"
process_each_entry_child = f"sweethometherapyllc_time_import_process_each_entry_date_child_{dag_id_prefix}"
process_each_inout_child = f"sweethometherapyllc_time_import_process_each_entry_child_{dag_id_prefix}"
process_log_generation = f"sweethometherapyllc_time_import_log_generation_{dag_id_prefix}"

can_run_batch_task = f"sweethometherapyllc_time_import_can_run_batch_task_{dag_id_prefix}_var"
