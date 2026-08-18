region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
pgp_conn_id = 'pgp_dxctechnology_ppmc_import'
sftp_conn_id = 'sftp_useast2'
input_filepath = '/Test/Inbound/PPMC/Input'
archive_filepath = '/Test/Inbound/PPMC/Archive'
log_filepath = '/Test/Inbound/PPMC/Logs'
project_task_import_child_dag_id = 'dxctechnology_ppmc_project_task_import_child_project_process'
task_user_details_report_name = 'PPMC Task- User Details report'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
dag_max_active_runs = 10
dag_max_active_tasks = 128

move_file_input_filepath = '/Test/Inbound/PPMC/Input'
move_file_process_filepath = '/Test/Inbound/PPMC/Processing'
move_file_archive_filepath = '/Test/Inbound/PPMC/Archive'
move_file_interval_in_minutes = 40
move_file_empty_file_size_in_bytes = 350
pgp_decrypt_empty_file_size_in_bytes = 600

sumo_conn_id = 'sumologic-notifications'
