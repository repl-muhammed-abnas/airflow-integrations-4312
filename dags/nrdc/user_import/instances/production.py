region = 'us-east-1'
environment = 'production'

instance = "NRDC"

company_key = 'NRDC'
replicon_conn_id = 'nrdc_replicon_admin'
can_run_batch_task_var_name = f'nrdc_user_import_usa_can_run_batch_task_{instance}'
user_report_name = '**User List For Email Notification**'
sftp_conn_id = "sftp_nrdc_639645"
sftp_conn_id2 = "sftp_gmailToSFTP_Integration_GmailtoSFTP"

input_filepath = "/NRDC/nrdc.userimport/Input"
archive_filepath = "/NRDC/nrdc.userimport/Archive"

execution_timeout_days = 14
child_dag_max_active_runs = 20
schedule_interval = 30
log_filepath = "/Logs"

tenant_email = "replicon.accountissues@nrdc.org"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
user_import_report_name = '***User Import Reference'
