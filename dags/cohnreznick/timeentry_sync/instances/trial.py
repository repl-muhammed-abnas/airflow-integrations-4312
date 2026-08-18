# pylint: disable=wildcard-import unused-wildcard-import
from cohnreznick.timeentry_sync.config import *
environment = "pre-production"

instance = "trial"
company_key = "cohnreznicktrial01"

replicon_conn_id = "cohnreznicktrial01_replicon_admin"
sftp_conn_id = "sftp_useast2"

input_filepath = "/Trial/Project time data/Input"
archive_filepath = "/Trial/Project time data/Archives"
log_filepath = "/Trial/Project time data/Logs"

main_dag_dagid = f'cohnreznick_time_entry_sync_master_dag_{instance}'
process_each_user_dagid = f'cohnreznick_time_entry_sync_process_each_unique_user_child_{instance}'
process_each_timeentry_for_user_dagid = f'cohnreznick_time_entry_sync_process_each_timeentry_for_user_child_{instance}'
process_log_generation = f'cohnreznick_time_entry_sync_child_process_log_generation_{instance}'

can_run_batch_task = f"cohnreznick_timesync_import_batch_task_variable_{instance}"
can_run_batch_task_master = f"cohnreznick_timesync_import_master_dag_batch_task_variable_{instance}"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
