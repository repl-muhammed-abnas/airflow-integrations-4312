from sandtechinc.timeoff_booking_import.config import *
environment = "pre-production"
instance = "uat"
company_key = "sandtechinctrial01"

replicon_conn_id = 'sandtechinctrial01_replicon_admin'
sftp_conn_id = 'sftp_sandtechinc_696582'

input_filepath_master = "/Trial/TimeOffImport/Input"
reference_filepath = "/Trial/TimeOffImport/Reference/"
archive_filepath = "/Trial/TimeOffImport/Archive"
archive_reference_filepath = "/Trial/TimeOffImport/Reference/Archive/"

tenant_email = "mhilburn@sandtech.com,jratcliffe@sandtech.com,ovanwyk@sandtech.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

main_dagid = f"{company_key}_timeoff_booking_import_master_{instance}"
process_timeoff_booking_child_dagid = f"{company_key}_timeoff_booking_import_child_{instance}"

can_run_batch_task_var_name = f"sandtechinc_timeoff_booking_import_child_{instance}_can_run_batch_task"
can_use_reference_file = f"sandtechinc_timeoff_booking_import_can_use_reference_file_{instance}"
