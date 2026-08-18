# pylint: disable=wildcard-import unused-wildcard-import
from cohnreznick.timeentry_sync.config import *

environment = "production"

instance = "prod"
company_key = "cohnreznick"

replicon_conn_id = "cohnreznick_replicon_repliconint.timeimport"
sftp_conn_id = "sftp_cohnreznick_640189"

input_filepath = "/Production/Project time data/Input"
archive_filepath = "/Production/Project time data/Archive"
log_filepath = "/Production/Project time data/Log"

main_dag_dagid = f'cohnreznick_time_entry_sync_master_dag_{instance}'
process_each_user_dagid = f'cohnreznick_time_entry_sync_process_each_unique_user_child_{instance}'
process_each_timeentry_for_user_dagid = f'cohnreznick_time_entry_sync_process_each_timeentry_for_user_child_{instance}'
process_log_generation = f'cohnreznick_time_entry_sync_child_process_log_generation_{instance}'

can_run_batch_task = f"cohnreznick_timesync_import_batch_task_variable_{instance}"
can_run_batch_task_master = f"cohnreznick_timesync_import_master_dag_batch_task_variable_{instance}"

tenant_email = "GovUnanetTime@cohnreznick.com,Ruth.Nalichowski@CohnReznick.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

process_users_child_max_active_run = 5
max_active_run_log_generation = 1
process_each_timesheet_max_active_run = 15
