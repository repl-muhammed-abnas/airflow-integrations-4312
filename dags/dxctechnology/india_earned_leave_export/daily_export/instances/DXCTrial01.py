region = 'us-east-2'
environment = 'pre-production'

instance = "dxctrial01"

company_key = 'DXCSandbox2'
replicon_conn_id = 'dxcsandbox2_replicon_RepliconIntWDPayroll'

can_run_batch_task_var_name = f'dxctrial01_india_earned_leave_export_can_run_batch_task_{instance}'

# only for QA testing
startdate_test_var_name = f'dxctechnology_india_earned_leave_export_startdate_{instance}'
enddate_test_var_name = f'dxctechnology_india_earned_leave_export_enddate_{instance}'


execution_timeout_days = 14
child_dag_max_active_runs = 10

sftp_conn_id = 'dxctechnology-ftp'
datafilepath = '/put'
logfilepath = '/put'

aws_conn_id = 'replicon.workato_S3_account'
# 'replicon-integrations-uswest'  # 'replicon-airflow-dev-group'
s3_bucket_name = 'replicon-integrations-uswest'

pgp_conn_id = 'pgp_dxctrial01_india_earned_leave_export'

schedule_time_zone = 'UTC'
schedule_interval = '30 01 * * *'  # 5AM IST Everyday

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
