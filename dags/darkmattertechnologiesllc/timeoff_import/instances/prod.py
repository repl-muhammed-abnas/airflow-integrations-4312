# pylint: disable=wildcard-import unused-wildcard-import
from darkmattertechnologiesllc.timeoff_import.config import *

instance = 'prod'
environment = 'production'

company_key = 'DarkMatterTechnologiesLLC'
replicon_conn_id = 'darkmattertechnologiesllc_replicon_admin'
pgp_conn_id = 'pgp_darkmattertechnologiesllc_userimport'

sftp_conn_id = 'sftp_darkmattertechnologiesllc_649288'

input_filepath = '/Production/Time Off/Input'
log_filepath = '/Production/Time Off/Log'
archive_filepath = '/Production/Time Off/Archive'

tenant_email = 'Operations@dmatter.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task = f'darkmatter_timeoff_sync_{instance}_can_run_batch_task'
can_decrypt_file = f'darkmatter_timeoff_sync_{instance}_can_decrypt_file'

master = f"darkmatter_timeoff_sync_master_{instance}"
process_timeoff_child = f"darkmatter_timeoff_sync_process_booking_timeoff_child_{instance}"
timeoff_booking_update_delete_child = f"darkmatter_timeoff_sync_booking_update_delete_child_{instance}"
timeoff_add_child = f"darkmatter_timeoff_sync_booking_add_child_{instance}"
