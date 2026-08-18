region = 'us-east-1'
environment = 'pre-production'

instance = "NRDCafmig"

company_key = 'NRDCafmig'
replicon_conn_id = 'NRDCafmig_UserImport'
can_run_batch_task_var_name = f'nrdc_user_import_usa_can_run_batch_task_{instance}'
user_report_name = '**User List For Email Notification**'
sftp_conn_id = "sftp_nrdcafmig_schedule_data_import"
sftp_conn_id2 = "sftp_nrdcafmig_schedule_data_import"

input_filepath = "/NRDCafmig/Input"
archive_filepath= "/NRDCafmig/Archive"

execution_timeout_days = 14
child_dag_max_active_runs = 20
# Everyday at Eastern Time (US & Canada)  "hour": "05", "minute": "00" - 10 am UTC
schedule_interval = 30
log_filepath = "/NRDCafmig/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
user_import_report_name = '***User Import Reference'
disabled = True
