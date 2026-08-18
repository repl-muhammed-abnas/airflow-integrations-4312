region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntCompass'
sftp_conn_id = 'sftp_dxc_compass_iwo_details'

input_filepath = '/Test/Inbound/COMPASSIWODetails/Processing'
archive_filepath = '/Test/Archive/COMPASSIWODetails'
log_filepath = '/Test/Logs/COMPASSIWODetails'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_max_active_runs = 1
child_dag_max_active_runs = 50
execution_timeout_days = 14

iwo_details_update_report = 'IWO Details update report'
time_zone = 'UTC'
schedule_interval = '0 */6 * * *'
first_delta = 12
second_delta = 6
reprocess_update_log = 'iwo_wbs_update_reprocess'
disabled = True
