from sandtechinc.timeoff_booking_import.config import *
environment = "pre-production"
instance = "trial"
company_key = "sandtechinctrial01"

replicon_conn_id = 'sandtechinctrial01_replicon_admin'
sftp_conn_id = 'sftp_useast2'

input_filepath_master = "/Trial/Time Off Import/Input"
reference_filepath = "/Trial/Time Off Import/Reference/"
archive_filepath = "/Trial/Time Off Import/Archive"
archive_reference_filepath = "/Trial/Time Off Import/Reference/Archive/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

main_dagid = f"{company_key}_timeoff_booking_import_master_{instance}"
process_timeoff_booking_child_dagid = f"{company_key}_timeoff_booking_import_child_{instance}"


can_run_batch_task_var_name = f"sandtechinc_timeoff_booking_import_child_{instance}_can_run_batch_task"
can_use_reference_file = f"sandtechinc_timeoff_booking_import_can_use_reference_file_{instance}"

## -- create OEF field for Time Off Booking with Booking_ID
## -- add blank reference file and archive folder for reference for Time Off Booking