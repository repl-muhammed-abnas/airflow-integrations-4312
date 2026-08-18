region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'test-sftp'
child_dag_max_active_runs = 10
execution_timeout_days = 14
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
usa_user_report_name = "USA user details - CSC  termination balance"
termination_balance_report_name = "Termination balance CSC report"
output_filepath = "/Test/dxctrail01/lcsc_termination_balance/"
log_filepath = "/Test/dxctrail01/lcsc_termination_balance/"

date_time_format = "%m/%d/%Y, %H:%M:%S"
encrypt_output_file_canada = False
encrypt_output_file_usa = False

tertiary_encrypted_filepath =''
tertiary_log_filepath = ''
tertiary_sftp_conn_id =''
tertiary_pgp_conn_id =''

# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'

can_upload_to_tertiary_sftp = False
