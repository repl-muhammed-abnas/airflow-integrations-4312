region = 'us-east-2'
environment = 'pre-production'

instance = "klatrial"

company_key = 'klatrial01'
replicon_conn_id = 'klatrial_UserImport'
can_run_batch_task_var_name = f'kla_user_import_usa_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_klatrial_schedule_data_import'
sftp_input_filepath = '/KLASandbox/Test'
sftp_ref_filepath = '/KLASandbox/reference'
sftp_archive_filepath = '/KLASandbox/Archive'
sftp_ref_file = '/KLASandbox/reference/newreference.csv'
sftp_ref_archive_path = '/KLASandbox/referencearchive'
sftp_log_filepath = '/KLASandbox/timeoffimportlogs'

execution_timeout_days = 14
child_dag_max_active_runs = 10

tenant_email = "DL-IT-Apps-Webapps@kla-tencor.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pto_prevent_balance_overdraw_amount = "40"
