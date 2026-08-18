region = 'us-east-1'
environment = 'production'

company_key = 'randstadlifesciences'
replicon_conn_id = 'randstadlifesciences-replicon-admin'

report_name = 'WeeklyExportHours'

sftp_conn_id = 'sftp_randstadlifesciences_replsftp'
upload_filepath = '/home/export'
schedule_interval = '0 1 * * 5'
eastern_timezone = 'America/New_York'

master_dag_max_active_runs = 1

execution_timeout_days = 14

alert_email = '{{ var.value.dagrun_internal_log_email }}'
