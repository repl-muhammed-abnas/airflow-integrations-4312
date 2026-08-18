region = 'us-east-1'
environment = 'production'

instance = "daimlertrucks"

company_key = 'daimlertrucks'
replicon_conn_id = 'daimlertrucks_replicon_replicon'

sftp_conn_id = 'sftp_daimlertrucks_540697'
sftp_processedrecords_directory = "/Production/Datamart/ENG/Timesheet/ProcessedRecords"
sftp_processing_directory = "/Production/Datamart/ENG/Processing"
sftp_rejectedrecords_directory = "/Production/Datamart/ENG/Timesheet/RejectedRecords"
sftp_archive_directory = "/Production/Datamart/ENG/Archive"

# only for QA testing
startdate_test_var_name = f'daimlertrucks_timesheet_export_startdate_{instance}'
enddate_test_var_name = f'daimlertrucks_timesheet_export_enddate_{instance}'

can_run_batch_task_var_name = f'daimlertrucks_timesheet_export_can_run_batch_task_{instance}'

execution_timeout_days = 14
child_dag_max_active_runs = 20

schedule_time_zone = 'PST8PDT'
schedule_interval = '0 2 * * 1-5'

tenant_email = "Replicon-Support@daimlertruck.com,dtna-eng-timewiz@daimlertruck.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
