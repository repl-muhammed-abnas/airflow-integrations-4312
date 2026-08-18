region = 'eu-central-1'
environment = 'pre-production'

instance = "youviewtvlimitedafmig"

company_key = 'youviewtvlimitedafmig'
replicon_conn_id = 'youviewtvlimitedafmig_replicon_admin'

can_run_batch_task_var_name = f'youviewtvlimited_user_import_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_youview_timeoff_deletion'
sftp_ref_file_path = "/TimeoffDeletion/Reference/timeoffreferencefile.csv"
sftp_archive_file_path = "/TimeoffDeletion/Archive"
sftp_input_filepath = '/TimeoffDeletion/Input'

execution_timeout_days = 14
child_dag_max_active_runs = 20

# repliconmailbox@youview.com
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled=True
