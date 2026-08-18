region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-compass'
sftp_conn_id = 'sftp_useast2'

input_filepath = '/Test/Inbound/COMPASSIWODetails/Input'
archive_filepath = '/Test/Inbound/COMPASSIWODetails/Archive'
log_filepath = '/Test/Inbound/COMPASSIWODetails/Logs'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_max_active_runs = 1
child_dag_max_active_runs = 5
execution_timeout_days = 14
master_dag_max_active_runs_reprocess = 1

iwo_details_update_report = 'IWO Details update report'
time_zone = 'UTC'
schedule_interval = '0 */6 * * *'
first_delta = 12
second_delta = 6
reprocess_update_log = 'iwo_wbs_update_reprocess'
can_run_batch_task_var_name = "compass_iwo_details_can_run_batch_task_var_name"
