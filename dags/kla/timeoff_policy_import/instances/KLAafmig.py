region = 'us-east-2'
environment = 'pre-production'

instance = "KLAafmig"

company_key = 'KLAafmig'
replicon_conn_id = 'KLAafmig_UserImport'
can_run_batch_task_var_name = f'kla_user_import_usa_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_klatrial_schedule_data_import'
sftp_input_filepath = '/KLAProduction'
sftp_ref_filepath = '/KLAProduction/reference'
sftp_archive_filepath = '/KLAProduction/Archive'
sftp_ref_file = '/KLAProduction/reference/newreference.csv'
sftp_ref_archive_path = '/KLAProduction/referencearchive'
sftp_log_filepath = '/timeoffimportlogs'

execution_timeout_days = 14
child_dag_max_active_runs = 10

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pto_prevent_balance_overdraw_amount = "80"

disable=True

disabled=True
