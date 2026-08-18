region = 'us-east-2'
environment = 'production'

instance = "dxctechnology"

company_key = 'dxctechnology'
replicon_conn_id = 'DXCTechnology_http_RepliconIntWDPayroll'

can_run_batch_task_var_name = f'DXC_india_earned_leave_export_can_run_batch_task_{instance}'

# only for QA testing
startdate_test_var_name = f'dxctechnology_india_earned_leave_export_startdate_{instance}'
enddate_test_var_name = f'dxctechnology_india_earned_leave_export_enddate_{instance}'


execution_timeout_days = 14
child_dag_max_active_runs = 10

sftp_conn_id = 'dxctechnology_ADP_LCSC_LES_US_export_SFTP'
datafilepath = '/put'
logfilepath = '/put'

aws_conn_id = 'replicon.workato_S3_account'
s3_bucket_name = 'replicon-integrations-useast'

pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'

schedule_time_zone = 'UTC'
schedule_interval = '30 23 * * *'  # 5AM IST Everyday

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
