# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.sap_h4s4_timeoff_booking_import_v1.config import *

instance = 'trial'
environment = 'pre-production'

version = '_v1'

company_key = 'BearingPointSandbox'
replicon_conn_id = "BearingPointSandbox_replicon_admin"

sftp_conn_id = 'sftp_internal_useast2'

input_filepath = '/bearingpoint/timeoff/input'
log_filepath = '/bearingpoint/timeoff/logs'
archive_filepath = '/bearingpoint/timeoff/archive'
sftp_reference_filepath = '/bearingpoint/timeoff/reference'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'bearingpoint_timeoff_sync_{instance}_can_run_batch_task'
can_decrypt_file = f'bearingpoint_timeoff_sync_{instance}_can_decrypt_file'

master_dag_id = f"bearingpoint_timeoff_booking_import_master_{instance}{version}"
process_each_user_dag_id = f"bearingpoint_timeoff_booking_import_process_each_user_child_{instance}{version}"
process_each_timeoff_booking = f"bearingpoint_timeoff_booking_import_process_each_timeoff_booking_child_{instance}{version}"
process_delete_timeoff_dag_id = f"bearingpoint_timeoff_booking_import_process_delete_timeoff_child_{instance}{version}"
