region = 'us-east-2'
environment = 'production'

instance = "KLA"

company_key = 'KLA'
replicon_conn_id = 'KLA-replicon-RNadmin'
can_run_batch_task_var_name = f'kla_user_import_usa_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_kla_210223'
sftp_input_filepath = '/KLAProduction'
sftp_ref_filepath = '/KLAProduction/reference'
sftp_archive_filepath = '/KLAProduction/Archive'
sftp_ref_file = '/KLAProduction/reference/newreference.csv'
sftp_ref_archive_path = '/KLAProduction/referencearchive'
sftp_log_filepath = '/timeoffimport'

execution_timeout_days = 14
child_dag_max_active_runs = 10

tenant_email = "Jim.nordin@kla.com,DL-IT-Apps-Webapps@kla-tencor.com,diana.wyland@kla-tencor.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pto_prevent_balance_overdraw_amount = "40"
