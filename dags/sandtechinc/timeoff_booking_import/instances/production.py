from sandtechinc.timeoff_booking_import.config import *
region = 'us-east-1'
environment = 'production'
instance = "prod"
company_key = "sandtechinc"

replicon_conn_id = 'sandtechinc_replicon_admin'
sftp_conn_id = 'sftp_sandtechinc_696582'

input_filepath_master = "/Production/TimeOffImport/Input"
reference_filepath = "/Production/TimeOffImport/Reference/"
archive_filepath = "/Production/TimeOffImport/Archive"
archive_reference_filepath = "/Production/TimeOffImport/Reference/Archive/"

tenant_email = "mhilburn@sandtech.com,jratcliffe@sandtech.com,ovanwyk@sandtech.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

main_dagid = f"{company_key}_timeoff_booking_import_master_{instance}"
process_timeoff_booking_child_dagid = f"{company_key}_timeoff_booking_import_child_{instance}"

can_run_batch_task_var_name = f"sandtechinc_timeoff_booking_import_child_{instance}_can_run_batch_task"
can_use_reference_file = f"sandtechinc_timeoff_booking_import_can_use_reference_file_{instance}"
