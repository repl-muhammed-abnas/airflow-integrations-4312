# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoff_hours_booking_import_v1.config import *

instance = 'uat'
environment = 'pre-production'

company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
pgp_conn_id = 'pgp_galaxyusopcoinctrial01_timeoffimport'

sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

input_filepath = '/Workday/WD TO Inbound/Test/Input'
log_filepath = '/Workday/WD TO Inbound/Test/Log'
archive_filepath = '/Workday/WD TO Inbound/Test/Archive'

tenant_email = 'utpal.chakraborty@vialto.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'vialto_timeoff_sync_{instance}_can_run_batch_task'
can_decrypt_file = f'vialto_timeoff_sync_{instance}_can_decrypt_file'

master_dag_id = f"vialto_timeoff_booking_import_master_{instance}_v1"
process_each_user_dag_id = f"vialto_timeoff_booking_import_process_each_user_child_{instance}_v1"
process_each_timeoff_booking = f"vialto_timeoff_booking_import_process_each_timeoff_booking_child_{instance}_v1"
