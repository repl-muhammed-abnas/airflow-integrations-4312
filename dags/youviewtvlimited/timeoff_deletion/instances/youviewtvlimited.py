region = 'eu-central-1'
environment = 'production'

instance = "youviewtvlimited"

company_key = 'youviewtvlimited'
replicon_conn_id = 'YouViewTVLimited_replicon_admin'

can_run_batch_task_var_name = f'youviewtvlimited_user_import_can_run_batch_task_{instance}'

sftp_conn_id = 'YouViewTVLimited_sftp_657934'
sftp_ref_file_path = "/TimeoffDeletion/Reference/timeoffreferencefile.csv"
sftp_archive_file_path = "/TimeoffDeletion/Archive"
sftp_input_filepath = '/TimeoffDeletion/Input'

execution_timeout_days = 14
child_dag_max_active_runs = 20

tenant_email = "repliconmailbox@youview.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = ""
