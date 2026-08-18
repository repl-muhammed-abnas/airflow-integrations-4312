region = 'us-east-1'
environment = 'pre-production'

instance = "DaimlerTrucksafmig"

company_key = 'DaimlerTrucksafmig'
replicon_conn_id = 'daimlertrucksafmig_replicon_admin'

# f"sftp_daimlertrucks_dta_eng_export_{instance}"
sftp_conn_id = 'rsftp-useast_for_testing'
sftp_processedrecords_directory = "/Production/Datamart/ENG/Timesheet/ProcessedRecords"
sftp_processing_directory = "/Production/Datamart/ENG/Processing"
sftp_rejectedrecords_directory = "/Production/Datamart/ENG/Timesheet/RejectedRecords"

# only for QA testing
startdate_test_var_name = f'daimlertrucks_timesheet_export_startdate_{instance}'
enddate_test_var_name = f'daimlertrucks_timesheet_export_enddate_{instance}'

sftp_archive_directory = "/Production/Datamart/ENG/Archive"

can_run_batch_task_var_name = f'daimlertrucks_timesheet_export_can_run_batch_task_{instance}'

execution_timeout_days = 14
child_dag_max_active_runs = 20

schedule_time_zone = 'PST8PDT'
schedule_interval = '0 2 * * 1-5'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
