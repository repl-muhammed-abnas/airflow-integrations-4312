region = 'us-east-1'
environment = 'production'

instance = "horizonmedia"

company_key = 'horizonmedia'
replicon_conn_id = 'horizonmedia_repliconadmin_replicon'

can_run_batch_task_var_name = 'horizonmedia_user_import_can_run_batch_task'

user_list_report_name = "User list - For Integration"

# true only for qa testing . set this to false on prod - default false
can_use_conf_payload_var_name = 'horizonmedia_user_import_can_use_conf_payload'
sftp_conn_id = "horizonmedia_client_sftp"
sftp_ref_file_path = "/User Sync/Reference/horizonmedia_reference.csv"
sftp_archive_file_path = "/User Sync/Archive"
logpath = '/User Sync/Log Files'

http_conn_id = "horizonmedia_http_user_sync"

execution_timeout_days = 14
child_dag_max_active_runs = 20

# "timezone": "America/New_York",
# "cron_expression": "30 3 * * 1-5"
schedule_time_zone = 'EST'
schedule_interval = '30 3 * * 1-5'

tenant_email = "gfraga@horizonmedia.com,Sgrandi@horizonmedia.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
