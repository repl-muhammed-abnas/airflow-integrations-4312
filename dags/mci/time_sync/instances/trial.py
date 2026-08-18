# pylint: disable=wildcard-import unused-wildcard-import
from mci.time_sync.config import *
from mci.time_sync.mapper.mci_time_sync_paycode_mapper import time_sync_timeoff_paycode_mapper

environment = 'pre-production'
instance = "trial"

company_key = 'MCIafmig'

replicon_conn_id = 'mciafmig_time_sync_admin'
http_conn_id = "paycom_http_time_sync"
sftp_conn_id = "sftp_internal"

input_filepath = "/shivam/mci/trial/timesync/Input"
archive_filepath= "/shivam/mci/trial/timesync/Archive"
log_filepath = "/timesync/logs"

timezone = "America/Los_Angeles"

schedule_interval = '0 10 * * *'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'mci_time_sync_can_run_batch_task_{instance}'

TIMEOFF_PAYCODE_MAPPER = time_sync_timeoff_paycode_mapper

mci_time_sync_master = f"mci_time_sync_master_{instance}"
mci_time_sync_puttimeentry_in_paycom_child = f"mci_time_sync_puttimeentry_in_paycom_child_{instance}"
process_log_generation = f"mci_time_sync_process_log_generation_child_{instance}"
