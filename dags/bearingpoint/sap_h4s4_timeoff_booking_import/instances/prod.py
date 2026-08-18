# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.sap_h4s4_timeoff_booking_import.config import *

instance = "prod"

environment = "production"

company_key = "bearingpointgmbh"

replicon_conn_id = "bearingpointgmbh_replicon_admin"

sftp_conn_id = 'sftp_bearingpointsandbox_539112'

input_filepath = '/PROD/Time Off/Input'
log_filepath = '/PROD/Time Off/Log'
archive_filepath = '/PROD/Time Off/Archived'
sftp_reference_filepath = '/PROD/Time Off/Reference'

tenant_email = 'work.smtp-0125104@bearingpoint.com,georgia.vasiliu@bearingpoint.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task = f'bearingpoint_timeoff_sync_{instance}_can_run_batch_task'
can_decrypt_file = f'bearingpoint_timeoff_sync_{instance}_can_decrypt_file'

master_dag_id = f"bearingpoint_timeoff_booking_import_master_{instance}"
process_each_user_dag_id = f"bearingpoint_timeoff_booking_import_process_each_user_child_{instance}"
process_each_timeoff_booking = f"bearingpoint_timeoff_booking_import_process_each_timeoff_booking_child_{instance}"
process_delete_timeoff_dag_id = f"bearingpoint_timeoff_booking_import_process_delete_timeoff_child_{instance}"
