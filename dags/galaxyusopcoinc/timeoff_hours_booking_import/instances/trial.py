# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoff_hours_booking_import.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
pgp_conn_id = 'pgp_galaxyusopcoinctrial01_timeoffimport'

sftp_conn_id = 'rsftp-useast_for_testing'

input_filepath = '/vialto/timeoff/input'
log_filepath = '/vialto/timeoff/logs'
archive_filepath = '/vialto/timeoff/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task = f'vialto_timeoff_sync_{instance}_can_run_batch_task'
can_decrypt_file = f'vialto_timeoff_sync_{instance}_can_decrypt_file'

master_dag_id = f"vialto_timeoff_booking_import_master_{instance}"
process_each_user_dag_id = f"vialto_timeoff_booking_import_process_each_user_child_{instance}"
process_each_timeoff_booking = f"vialto_timeoff_booking_import_process_each_timeoff_booking_child_{instance}"
disabled = True
