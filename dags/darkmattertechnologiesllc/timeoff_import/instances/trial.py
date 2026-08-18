# pylint: disable=wildcard-import unused-wildcard-import
from darkmattertechnologiesllc.timeoff_import.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'DarkMatterTechnologiesLLCTrial01'
replicon_conn_id = 'darkmattertechnologiesllctrial01_replicon_admin'
pgp_conn_id = 'pgp_darkmattertechnologiesllc_timeoffimport'

sftp_conn_id = 'sftp_internal'

input_filepath = '/shivam/darkmatter/timeoff/input'
log_filepath = '/shivam/darkmatter/timeoff/logs'
archive_filepath = '/shivam/darkmatter/timeoff/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task = f'darkmatter_timeoff_sync_{instance}_can_run_batch_task'
can_decrypt_file = f'darkmatter_timeoff_sync_{instance}_can_decrypt_file'

master = f"darkmatter_timeoff_sync_master_{instance}"
process_timeoff_child = f"darkmatter_timeoff_sync_process_booking_timeoff_child_{instance}"
timeoff_booking_update_delete_child = f"darkmatter_timeoff_sync_booking_update_delete_child_{instance}"
timeoff_add_child = f"darkmatter_timeoff_sync_booking_add_child_{instance}"

disabled=True
