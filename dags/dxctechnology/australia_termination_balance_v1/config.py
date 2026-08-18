region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'test-sftp'
child_dag_max_active_runs = 16
max_active_dag_runs= 1
execution_timeout_days = 14
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
usa_user_report_name = "USA user details - AUS termination balance"
termination_balance_report_name = "Termination balance AUS report"
output_filepath = "/Test/dxctrail01/lcsc_termination_balance/"
log_filepath = "/Test/dxctrail01/lcsc_termination_balance/"

date_time_format = "%m/%d/%Y, %H:%M:%S"
encrypt_output_file_canada = False
encrypt_output_file_usa = False

cutoff_date = "2023-07-01"

# pylint: disable=line-too-long
error_template = '{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") | attr_or_default(["response.body", "exc_message", ""], default="Unknown error occurred") }}'
disabled = True
